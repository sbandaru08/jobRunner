// get large file md5 hash on the fly with copying 

const crypto = require('crypto');
const ParallelJobQueue = require('../jobRunner');

const fs = require('fs');
const path = require('path');

function _open(fname,flag){
    return new Promise((resolve,reject) => {
        fs.open(fname, flag, (err,fd) => {
            if(err) {
                console.log(err);
                reject(err);
            }
            resolve(fd);
        })
    })
}

function _stat(fname) {
    return new Promise((resolve,reject) => {
        fs.stat(fname, (err,stats) => {
            if(err) reject(err);
            resolve(stats);
        })
    })
}

function _read(readFD, buff, readOffset) {
    return new Promise((resolve,reject) => {
        console.log(buff.length)
        fs.read(readFD, buff, 0, buff.length, readOffset, (err,bytesRead,outBuff) => {
            if(err) reject(err);
            resolve({bytesRead:bytesRead, outBuff : outBuff});
        })
    })
}

function _write(writeFD, buff, dataLength, writeOffset) {
    return new Promise((resolve,reject) => {
        fs.write(writeFD, buff, 0, dataLength, writeOffset, (err,bytesWrite,outBuff) => {
            if(err) reject(err);
            resolve({bytesRead:bytesWrite, outBuff : outBuff});
        })
    })
}

function _sliceFileByCount(fsize,sliceCount){
    const chunkBytes = Math.ceil(fsize/sliceCount);
    console.log(chunkBytes)
    const slices = [];
    let count = sliceCount;
    let startBytes = 0;
    let endBytes;
    let partNum = 1;
    while(count >0){
        const slice = {"partNum" : partNum};
        slice.start = startBytes;
        endBytes = startBytes + chunkBytes;
        if( endBytes > fsize ){
            endBytes = fsize;
        }
        slice.end = endBytes;
        slice.size = endBytes - startBytes;
        startBytes = endBytes;

        slices.push(slice)
        partNum ++
        count --        
    }
    return slices
}


function readChunk(args){
    const {readFD,start,end} = args;
    return new Promise(async (resolve,reject) => {
        try {
            const buff = Buffer.alloc(end - start);
            const {bytesRead,outBuff} = await _read(readFD, buff, start);
            resolve(outBuff);
        } catch (err) {
            console.error(err);
            reject('err');
        }
    })
}

function sortByKey(data, key){
    data.sort(function (a, b) {
        if (a[key] > b[key]) {
          return 1;
        }
        if (a[key] < b[key]) {
          return -1;
        }
    });          
}

async function main(){
    const md5hash = crypto.createHash('md5');
    const srcFile = 'c:/temp/mp4.mp4';
    //const srcFile = 'D:/098.Virtual_Box/SIMEP/SIMEP1_20130130.vhd'
    const readFD  = await _open(srcFile, 'r');
    const tempJobOutput = {};
    let nextMD5UpdateJobNum = 1;
    
    const srcStats = await _stat(srcFile);
    const srcSize = srcStats.size;

    const sliceInfo = _sliceFileByCount(srcSize,1000); // [{partNum:1, start:0, end:1024}..]

    // define jobQueue
    const options = {
        saveJobResults : false,
        stopOnJobFailed : true,
    }
 
    const jobs = new ParallelJobQueue((results) => {
        fs.closeSync(readFD);
        console.log(md5hash.digest('hex'));
        console.log(new Date())
        console.log('close FD all done');

    }, options);

    const JOBTIMEOUTSEC = 3600;
    sliceInfo.map((slice) => {
        const args = {
            readFD : readFD,
            start : slice.start,
            end : slice.end            
        }
        jobs.addJob(readChunk, args, JOBTIMEOUTSEC);
    })

    console.log(new Date())
    jobs.start(10);

    jobs.on('jobDone', (readBuff, job) => {
        console.log(`length of temp job output : ${Object.keys(tempJobOutput).length}`);
        if(job.jobNum == nextMD5UpdateJobNum){
            md5hash.update(readBuff);
            nextMD5UpdateJobNum++;  
            processTempJobOutput();          
        } else {
            console.log(`expect jobNum = ${nextMD5UpdateJobNum}, done jobNum = ${job.jobNum}`)
            tempJobOutput[job.jobNum] = readBuff;
        } 
    })

    function processTempJobOutput() {
        if(tempJobOutput[nextMD5UpdateJobNum]){
            md5hash.update(tempJobOutput[nextMD5UpdateJobNum]);
            delete tempJobOutput[nextMD5UpdateJobNum];
            nextMD5UpdateJobNum++;
            processTempJobOutput()
        }
    }
}

main();
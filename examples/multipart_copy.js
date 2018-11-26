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


function rwFileOffset(args){
    const {readFD,writeFD,start,end} = args;
    return new Promise(async (resolve,reject) => {
        try {
            const buff = Buffer.alloc(end - start);
            const {bytesRead,outBuff} = await _read(readFD, buff, start);
            await _write(writeFD, outBuff, bytesRead, start);
            resolve('done');
        } catch (err) {
            console.error(err);
            reject('err');
        }
    })
}

async function main(){
    const srcFile = 'c:/temp/mp4.mp4';
    const dstFile = 'c:/temp/mp4_clone.mp4';
    const readFD  = await _open(srcFile, 'r');
    const writeFD = await _open(dstFile, 'w');
    
    const srcStats = await _stat(srcFile);
    const srcSize = srcStats.size;

    const sliceInfo = _sliceFileByCount(srcSize,50); // [{partNum:1, start:0, end:1024}..]

    // define jobQueue
    const options = {
        saveJobResults : false,
        stopOnJobFailed : true,
    }
 
    const jobs = new ParallelJobQueue((results) => {
        fs.closeSync(readFD);
        fs.closeSync(writeFD);
        console.log('close FD all done');
    }, options);

    const JOBTIMEOUTSEC = 3600;
    sliceInfo.map((slice) => {
        const args = {
            readFD : readFD,
            writeFD : writeFD,
            start : slice.start,
            end : slice.end
        }
        jobs.addJob(rwFileOffset, args, JOBTIMEOUTSEC);
    })

    jobs.start(10);

}

main();
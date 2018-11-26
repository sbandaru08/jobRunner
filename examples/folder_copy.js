// copy file sample

// 1. read directory (fs.readdir())
// 2. make files array
// 3. copy each file to destDirectory concurrently


const fs = require('fs');
const path = require('path');
const ParallelJobQueue = require('../jobRunner');
const srcDir = 'c:/temp';
const dstDir = 'c:/temp1';
const srcFiles = [];

function _readDir(srcDir) {
    return new Promise((resolve,reject) => {
        fs.readdir(srcDir, (err,files) => {
            if(err) reject(err);
            resolve(files);
        })
    })
}

function _stat(fullname) {
    return new Promise((resolve,reject) => {
        fs.stat(fullname, (err,stats) => {
            if(err) console.error(err);
            resolve(stats);
        })
    })
}

const copyFile = function(args) {
    console.log(`copyFile : start : ${args.srcFile}`);
    const {srcFile,dstFile} = args;
    return new Promise((resolve,reject) => {
        fs.copyFile(srcFile, dstFile, (err) => {
            if(err) reject(err);
            resolve(`copy from ${srcFile} to ${dstFile} succeed`);
        })
    })
}


async function main(){
    const files = await _readDir(srcDir);
    const subFiles = [];
    while(files.length > 0) {
        const file = files.shift();
        const fullname = path.join(srcDir,file);
        const stats = await _stat(fullname);
        if(stats.isFile()) {
            subFiles.push(file)
        }
    }

    // define jobQueue
    const options = {
        saveJobResults : false,
        stopOnJobFailed : true,
    }

    const jobQueue = new ParallelJobQueue(lastCallback = console.log, options);
    const JOBTIMEOUTSEC = 3600;
    subFiles.map((file) => {
        const args = {};
        args.srcFile = path.join(srcDir, file);
        args.dstFile = path.join(dstDir, file);
        jobQueue.addJob(copyFile, args, JOBTIMEOUTSEC)    
    })
    jobQueue.start(2);

    jobQueue.on('jobDone', (result, job) => {
        console.log(`${job.args.srcFile} copy to ${job.args.dstFile} done [${job.jobNum}]`);
    })
    
    jobQueue.on('jobError', (err, job) => {
        console.log(`${job.args.srcFile} copy to ${job.args.dstFile} fail [${job.jobNum}]`);
    })
    
    jobQueue.on('jobTimeOut', (err, job) => {
        console.log(`${job.args.srcFile} copy to ${job.args.dstFile} TimeOut [${job.jobNum}]`);
    })
}

main()








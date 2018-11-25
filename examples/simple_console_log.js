// simple log sample 
const ParallelJobQueue = require('../jobRunner');

// define job
const delayedLog = function(sleepTime){
    return new Promise((resolve,reject) => {
        setTimeout(() => {
            console.log(`sleep ${sleepTime}`);
            resolve(`success`)           
        },sleepTime)
    });
}

// define jobQueue
const options = {
    saveJobResults : true,
    stopOnJobFailed : true,
}
const jobQueue = new ParallelJobQueue(lastCallback=console.log, options);


// add job to jobQueue
let count = 1;
const SLEEPTIME = 2000;
const JOBTIMEOUTSEC = 5;
while(count < 10){
    jobQueue.addJob(delayedLog, SLEEPTIME, JOBTIMEOUTSEC);
    count ++;
}

// jobQueue start with councurrency
jobQueue.start(5);

jobQueue.on('jobDone', (result, job) => {
    console.log(`job resolved [${job.jobNum}]`);
    setTimeout(() => {
        console.log(`resolve callback Done [${job.jobNum}]`);        
    },1000)
})

jobQueue.on('jobError', (err, job) => {
    console.log(`job rejected [${job.jobNum}]`);
})

jobQueue.on('jobTimeOut', (err, job) => {
    console.log(`job TimeOut [${job.jobNum}]`);
})

// copy file sample

// 1. read directory (fs.readdir())
// 2. make files array
// 3. copy each file to destDirectory concurrently

/*
const fs = require('fs');
const path = require('path');
const srcDir = 'c:/temp';
const dstDir = 'c:/AWS/temp';

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

    const jobQueue = new ParallelJobQueue(lastCallback = console.log);
    subFiles.map((file) => {
        const args = {};
        args.srcFile = path.join(srcDir, file);
        args.dstFile = path.join(dstDir, file);
        jobQueue.addJob(copyFile, args, console.log, console.error)    
    })
    jobQueue.start(2);

    // all subFiles elements become pending Promise. why?
    // const subFiles = files.map( async (file) => {
    //    const fullname = path.join(srcDir,file);
    //    const stats = await _stat(fullname);
    //    if(stats.isFile()) {
    //        return fullname
    //    }
    //})
}

main()
*/







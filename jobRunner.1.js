/*
Usage 

1) Make Job Function as Promise
   * args can be object, string or array 
   function job(args, callback){       
       const {x,y,z} = args;
       return new Promise(...{

       })
   }

2) Set new ParallelJobQueue
   @lastCallback : callback function called when all jobs done 
   @saveJobResult (true/false) : default false
                  whether accumulate all promised job's result on totalResult Array
                  "true" can make out of memory
                  "false" convert job's result small object({jobNum:n, success:true}) and
                  push to totalResult Array ( no OOM )
   @stopOnJobFailed (true/false) : default true
   const jobQueue = new ParallelJobQueue(lastCallback, saveJobResult=false, stopOnError=false)

3) Add job function to jobQueue
   @job : job function
   @args : argument for job (object), if you need multiple argument, use object;
   
   jobQueue.addJob(job, args) 
   
   * if job function is class method and need access "this", use bind   
   jobQueue.addJob(job.bind(this), args) 


4) at last, run job with concurrency constant
   jobQueue.start(5)  

5) events
   jobQueue.on('jobDone', (jobResult, jobObj) => {})
   jobQueue.on('jobError', (error, jobObj) => {})
*/

const EventEmitter = require('events');

class Job {   

    constructor(jobFunction, args, jobNum){
        this.jobFunction = jobFunction;
        this.args = args;
        this.jobNum = jobNum;
        this.running = false;
        this.paused = false;
        this.success = undefined;
    }

    start() {
        try {
            return this.jobFunction(this.args);
        } catch (err) {
            return err;
        }
    }

    setRunning(){
        this.running = true;
    }
    setResolved(){        
        this.running = false;
        this.success = true;
    }
    setRejected(){        
        this.running = false;
        this.success = false;
    }
    isRunning(){
        return this.running;
    }
}


class ParallelJobQueue extends EventEmitter {

    constructor(lastCallback, options){
        super();
        this.jobLength = 0;
        this.jobsToRun = [];
        this.jobsRunning = [];
        this.lastCallback = lastCallback;   
        this.totalResults = [];  
        this.paused = false;
        this.cancelled = false;
        if (options && typeof(options) === 'object') {
            this.saveJobResults = options.hasOwnProperty('saveJobResults') ? options.saveJobResults : false;
            this.stopOnJobFailed = options.hasOwnProperty('stopOnJobFailed') ? options.stopOnJobFailed : true;
            this.jobTimeOutSec = options.hasOwnProperty('jobTimeOutSec') ? options.stopOnJobFailed : 10;
        } else {
            this.saveJobResults = false;
            this.stopOnJobFailed = true;
        }
    }

    addJob(jobFunction, args){
        try {
            const jobNum = this.jobLength + 1;
            const job = new Job(jobFunction, args, jobNum);
            this.jobsToRun.push(job);    
            this.jobLength ++ ;
        } catch (err) {
            console.error(err)
        } 
    }
    
    _runParallel(job) {
        return new Promise( async (resolve,reject) => {
            this._addToRunning(job);            
            console.log(`job start jobNum = [${job.jobNum}]`);
            try {
                job.setRunning();
                const jobResult = await job.start();            
                console.log(`job done jobNum = [${job.jobNum}]`); 
                this.emit('jobDone', jobResult, job); // jobDone but job Callback not yet done!
                job.setResolved();
                const resultToSave = {jobNum:job.jobNum, success:true};
                this.saveJobResults ? resultToSave.jobResult = jobResult : resultToSave.jobResult = undefined;
                this._addTotalResult(resultToSave);             
                this._delFromRunning(job.jobNum);

            } catch (err) {
                console.error(`job failed jobNum = [${job.jobNum}]`);
                console.error(err)
                this.emit('jobError', err, job);
                job.setRejected();
                this._addTotalResult({jobNum:job.jobNum, success:false});   
                this._delFromRunning(job.jobNum);
                if(this.stopOnJobFailed) this.cancel();
                /*
                if(err.code !== 'RequestAbortedError'){
                    this.saveJobResults ? this._addTotalResult(err) : this._addTotalResult({jobNum:job.jobNum, success:false});
                }
                */
            } finally {
                if(this.jobsToRun.length > 0 && !this.paused && !this.cancelled){
                    const nextJob = this.jobsToRun.shift();
                    this._runParallel(nextJob);
                }   
                this._isAllJobDone() ? this.lastCallback(this.totalResults) : this._notifyProgress(); 
            }
        })
    }
    

    _notifyProgress() {
        console.log(`count of jobs processed : ${this.totalResults.length}`);    
        console.log(`count of jobs successed : ${this._getSuccessedJob().length}`);
        console.log(`count of jobs failed : ${this._getFailedJob().length}`);
    }

    _getSuccessedJob(){
        return this.totalResults.filter((result) => {
            return result.success;
        })
    }

    _getFailedJob(){
        return this.totalResults.filter((result) => {
            return !result.success;
        })
    }

    _addToRunning(job){
        this.jobsRunning.push(job);
    }

    _getJobsRunning(){ 
        return this.jobsRunning;
    }

    _delFromRunning(jobNum){
        const index = this.jobsRunning.findIndex((job) => {
            return job.jobNum == jobNum;
        })
        delete this.jobsRunning.splice(index,1);
    }

    _addTotalResult(result){
        this.totalResults.push(result);
    }

    _isAllJobDone(){
        return (this.totalResults.length === this.jobLength);
    }

    async start(concurrency){
        while(this.jobsToRun.length > 0 && !this.cancelled && this.jobsRunning.length < concurrency){
            const job = this.jobsToRun.shift();
            this._runParallel(job);
        }          
    }

    pause(){
        this.paused = true;
    }

    cancel(){
        this.cancelled = true;
    }

    resume(concurrency){
        this.paused = false;
        this.jobsToRun = [...this.jobsRunning,...this.jobsToRun];
        this.jobsRunning = [];
        while(this.jobsToRun.length > 0 && this.jobsRunning.length < concurrency){
            const job = this.jobsToRun.shift();
            this._runParallel(job);
        }
    }
}

module.exports = ParallelJobQueue;

/*
// simple setTimeout log sample 

const delayedLog = function(args){
    const {sleepTime} = args;
    return new Promise((resolve,reject) => {
        setTimeout(() => {
            console.log(`sleep ${sleepTime}`);
            resolve(`done`)
        },sleepTime)
    });
}

const jobQueue = new ParallelJobQueue(lastCallback=console.log);

let count = 1;

// add job to jobQueue
while(count < 100){
    args = {sleepTime:1000};
    jobQueue.addJob(delayedLog, args);
    count ++;
}

jobQueue.start(10);

jobQueue.on('jobDone', (result, job) => {
    console.log('start resolve callback');
    setTimeout(() => {
        console.log(`resolve callback Done ${job.jobNum}`);        
    },2000)
})

jobQueue.on('jobError', (error, job) => {
    console.log('start reject callback');

})

*/
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







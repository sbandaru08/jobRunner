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
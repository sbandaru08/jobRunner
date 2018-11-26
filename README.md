# jobRunner
This is parallel task runner base on javascript ( node.js and browser )

## Feature
- simple usage
- can add your custom event listener to repeateed job
- can choose where stop on any job failure
- can use all job's results as array
```javascript
{
    jobNum:number of job, 
    success:true/false, 
    jobResult:result of job 
    // jobResult can make out of memory exception.
    // only provided if you set "saveJobResults" as true
}
```

## Usage

1. git clone
```bash
# Clone this repository
git clone https://github.com/ryuken73/jobRunner.git
```
2. import module
```javascript
const ParallelJobQueue = require('../jobRunner');
```

3. make new jobQueue instance
```javascript
const options = {
    saveJobResults : true, 
    //save each job's total result
    //total result will be provided to lastcallback 
    //Be carefull. this can make large memory llocation
    //process each jobs result in 'jobDone' listener
    //example) multipart_md5.js
    stopOnJobFailed : true,
    //when one job failed, stop jobQueue
}
const jobQueue = new ParallelJobQueue(lastCallback=console.log, options);
```

4. create your repeated job function as promised
```javascript
const delayedLog = function(sleepTime){
    return new Promise((resolve,reject) => {
        setTimeout(() => {
            console.log(`sleep ${sleepTime}`);
            resolve(`success`)           
        },sleepTime)
    });
}
```

5. add your job function to jobQueue
```javascript
let count = 1;
const SLEEPTIME = 2000;
const JOBTIMEOUTSEC = 5;

// add 10 jobs to run
while(count < 10){
    jobQueue.addJob(delayedLog, SLEEPTIME, JOBTIMEOUTSEC);
    count ++;
}
```

6. start your jobs with concurrency
```javascript
jobQueue.start(5); // concurrency 5
```

7. add listener to your job 
```javascript
// each job's done callback
jobQueue.on('jobDone', (result, job) => {
    console.log(`job resolved [${job.jobNum}]`);
})
// each job's error callback
jobQueue.on('jobError', (err, job) => {
    console.log(`job rejected [${job.jobNum}]`);
})
// each job's timeout callback
jobQueue.on('jobTimeOut', (err, job) => {
    console.log(`job TimeOut [${job.jobNum}]`);
})
```

## Examples

1. simple_console_log.js
2. folder_copy.js
3. multipart_cpopy.js
4. multipart_md5.js

## License

[CC0 1.0 (Public Domain)](LICENSE.md)
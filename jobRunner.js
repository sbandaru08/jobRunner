const EventEmitter = require('events');

// Job class handles individual jobs
class Job {
    constructor(jobFunction, args, jobNum, jobTimeOutSec = 3600) {
        this.jobFunction = jobFunction;
        this.args = args;
        this.jobNum = jobNum;
        this.running = false;
        this.success = undefined;
        this.jobTimeOutSec = jobTimeOutSec;
        this.timer = null;
        this.logger = global.logger || console;
    }

    async start() {
        const timerPromise = new Promise((_, reject) => {
            this.timer = setTimeout(() => {
                this.logger.error('Job timed out');
                reject({ code: 'TIMEOUT', message: `Job execution exceeded timeout of ${this.jobTimeOutSec} seconds` });
            }, this.jobTimeOutSec * 1000);
        });

        try {
            const result = await Promise.race([this.jobFunction(this.args), timerPromise]);
            clearTimeout(this.timer);
            return result;
        } catch (error) {
            clearTimeout(this.timer);
            throw error;
        }
    }

    setRunning() {
        this.running = true;
    }

    setResolved() {
        this.running = false;
        this.success = true;
    }

    setRejected() {
        this.running = false;
        this.success = false;
    }

    isRunning() {
        return this.running;
    }
}

// ParallelJobQueue manages the execution of multiple jobs
class ParallelJobQueue extends EventEmitter {
    constructor(lastCallback, options = {}) {
        super();
        this.jobsToRun = [];
        this.jobsRunning = [];
        this.totalResults = [];
        this.lastCallback = lastCallback;
        this.paused = false;
        this.cancelled = false;
        this.logger = global.logger || console;

        // Configuration for handling results and failure behaviors
        this.saveJobResults = options.saveJobResults || false;
        this.stopOnJobFailed = options.stopOnJobFailed !== undefined ? options.stopOnJobFailed : true;
    }

    addJob(jobFunction, args, jobTimeOutSec) {
        const jobNum = this.jobsToRun.length + 1;
        const job = new Job(jobFunction, args, jobNum, jobTimeOutSec);
        this.jobsToRun.push(job);
    }

    async _runJob(job) {
        this._addToRunning(job);
        job.setRunning();

        try {
            const jobResult = await job.start();
            this.logger.info(`Job ${job.jobNum} done`);
            this.emit('jobDone', jobResult, job);
            job.setResolved();
            this._addTotalResult({ jobNum: job.jobNum, success: true, jobResult });
        } catch (err) {
            this.logger.error(`Job ${job.jobNum} failed, Error: ${err.message}`);
            this.emit('jobError', err, job);
            job.setRejected();
            this._addTotalResult({ jobNum: job.jobNum, success: false });

            if (this.stopOnJobFailed) {
                this.cancel();
            }
        } finally {
            this._delFromRunning(job.jobNum);

            if (this.jobsToRun.length > 0 && !this.paused && !this.cancelled) {
                this._processNextJob();
            }

            this._isAllJobDone() ? this.lastCallback(this.totalResults) : this._notifyProgress();
        }
    }

    _processNextJob() {
        const nextJob = this.jobsToRun.shift();
        this._runJob(nextJob);
    }

    _notifyProgress() {
        const progress = `Processed: ${this.totalResults.length}, Success: ${this._getSuccessedJob().length}, Failure: ${this._getFailedJob().length}`;
        // this.logger.trace(`Job progress: ${progress}`);
    }

    _getSuccessedJob() {
        return this.totalResults.filter(result => result.success);
    }

    _getFailedJob() {
        return this.totalResults.filter(result => !result.success);
    }

    _addToRunning(job) {
        this.jobsRunning.push(job);
    }

    _delFromRunning(jobNum) {
        const index = this.jobsRunning.findIndex(job => job.jobNum === jobNum);
        if (index >= 0) {
            this.jobsRunning.splice(index, 1);
        }
    }

    _addTotalResult(result) {
        this.totalResults.push(result);
    }

    _isAllJobDone() {
        return this.totalResults.length === this.jobsToRun.length;
    }

    start(concurrency) {
        const jobsToRun = Math.min(concurrency, this.jobsToRun.length);

        for (let i = 0; i < jobsToRun; i++) {
            this._processNextJob();
        }
    }

    pause() {
        this.paused = true;
    }

    cancel() {
        this.cancelled = true;
    }

    resume(concurrency) {
        this.paused = false;
        while (this.jobsToRun.length > 0 && this.jobsRunning.length < concurrency) {
            this._processNextJob();
        }
    }
}

module.exports = ParallelJobQueue;

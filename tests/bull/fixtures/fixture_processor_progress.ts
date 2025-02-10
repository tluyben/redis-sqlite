/**
 * A processor file to be used in tests.
 */
import delay from "delay";
import { Job } from "bull";

export default function(job: Job): Promise<number> {
  return delay(50)
    .then(() => {
      job.progress(10);
      job.log(job.progress().toString());
      return delay(100);
    })
    .then(() => {
      job.progress(27);
      job.log(job.progress().toString());
      return delay(150);
    })
    .then(() => {
      job.progress(78);
      job.log(job.progress().toString());
      return delay(100);
    })
    .then(() => {
      job.progress(100);
      job.log(job.progress().toString());
    })
    .then(() => {
      return 37;
    });
}

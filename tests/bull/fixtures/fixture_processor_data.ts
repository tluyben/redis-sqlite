/**
 * A processor file to be used in tests.
 */
import delay from "delay";
import { Job } from "bull";

interface JobData {
  baz?: string;
  [key: string]: any;
}

export default function(job: Job<JobData>): Promise<JobData> {
  return delay(50).then(() => {
    job.update({ baz: "qux" });
    return job.data;
  });
}

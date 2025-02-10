/**
 * A processor file to be used in tests.
 */
import delay from "delay";
import { Job } from "bull";

export default function(job: Job): Promise<never> {
  return delay(500).then(() => {
    job.discard();
    throw new Error("Manually discarded processor");
  });
}

/**
 * A processor file to be used in tests.
 */
import delay from "delay";
import { Job, DoneCallback } from "bull";

export default function(job: Job, done: DoneCallback): void {
  delay(500).then(() => {
    done(new Error("Manually failed processor"));
  });
}

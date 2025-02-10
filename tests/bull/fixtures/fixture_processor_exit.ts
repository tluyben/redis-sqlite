/**
 * A processor file to be used in tests.
 */
import delay from "delay";
import { Job } from "bull";

export default function(job?: Job): Promise<void> {
  return delay(500).then(() => {
    delay(100).then(() => {
      process.exit(0);
    });
  });
}

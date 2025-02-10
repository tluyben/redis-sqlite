/**
 * A processor file to be used in tests.
 */
import delay from "delay";
import { Job } from "bull";

export default function(job?: Job): Promise<number> {
  return delay(500).then(() => {
    return 42;
  });
}

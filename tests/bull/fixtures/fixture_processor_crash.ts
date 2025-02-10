/**
 * A processor file to be used in tests.
 */
import { Job } from "bull";

interface CrashJobData {
  exitCode?: number;
}

export default function(job: Job<CrashJobData>): Promise<never> {
  setTimeout(() => {
    if (typeof job.data.exitCode !== "number") {
      throw new Error("boom!");
    }
    process.exit(job.data.exitCode);
  }, 100);

  return new Promise(() => {});
}

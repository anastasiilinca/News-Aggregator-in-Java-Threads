import subprocess
import time

# Path components (modify if needed)
classpath = ".:lib/*"          # Use ".;lib/*" on Windows!
program = "Tema1"

# Input files
file1 = "../checker/input/tests/test_3/articles.txt"
file2 = "../checker/input/tests/test_3/inputs.txt"

print("Running tests...\n")

# Loop through number of threads
for threads in range(1, 17):
    print(f"=== Running with {threads} threads ===")

    times = []  # store duration of each run

    # Run 3 times
    for run in range(1, 4):
        print(f"--- Run {run} ---")

        cmd = [
            "java", "-cp", classpath,
            program, str(threads), file1, file2
        ]

        start = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        end = time.time()

        duration = end - start
        times.append(duration)

        print(result.stdout)
        if result.stderr:
            print("Errors:", result.stderr)

        print(f"Time: {duration:.4f} seconds")

    # Compute average time
    avg_time = sum(times) / len(times)
    print(f">>> Average time for {threads} threads: {avg_time:.4f} seconds\n")

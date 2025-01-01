from multiprocessing import Process, Value

class Process:
    def __init__(self, num_processes = 5):
        if __name__ == "__main__":
            shared_value = Value('i', 0)
            processes = [Process(target=self.worker, args=(shared_value, i)) for i in range(num_processes)]
            monitor_process = Process(target=self.monitor_progress, args=(shared_value, num_processes))
            for p in processes: p.start()
            monitor_process.start()

    def worker(shared_value, index):
        shared_value.value += 1


    def monitor_progress(shared_value, num_processes):
        progress = 0
        while progress < num_processes:
            progress = shared_value.value
            print(f"Progress: {(progress / num_processes) * 100}%")
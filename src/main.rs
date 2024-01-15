use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

type Message = String;

trait Subscriber {
    fn process(&self, message: &Message);
}

type SubscriberBox = Box<dyn Subscriber + Send>;
type SubscriberList = Vec<SubscriberBox>;
type SubscriberMap = HashMap<String, SubscriberList>;
type ThreadSafeSubscriberMap = Arc<Mutex<SubscriberMap>>;

struct Publisher {
    subscribers: ThreadSafeSubscriberMap,
}

impl Publisher {
    fn new() -> Self {
        Publisher {
            subscribers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn subscribe(&mut self, topic: String, subscriber: Box<dyn Subscriber + Send>) {
        let mut subs = self.subscribers.lock().unwrap();
        subs.entry(topic).or_insert_with(Vec::new).push(subscriber);
    }

    fn publish(&self, topic: &str, message: &Message) {
        let subs = self.subscribers.lock().unwrap();
        if let Some(subscribers) = subs.get(topic) {
            for subscriber in subscribers {
                subscriber.process(message);
            }
        }
    }
}

struct ConcreteSubscriber;

impl Subscriber for ConcreteSubscriber {
    fn process(&self, message: &Message) {
        println!("Received message: {}", message);
    }
}

type Job = Box<dyn FnOnce() + Send + 'static>;

struct JobQueue {
    jobs: Mutex<VecDeque<Job>>,
    available: Condvar,
}

impl JobQueue {
    fn new() -> Arc<JobQueue> {
        Arc::new(JobQueue {
            jobs: Mutex::new(VecDeque::new()),
            available: Condvar::new(),
        })
    }

    fn add_job(&self, job: Job) {
        let mut jobs = self.jobs.lock().unwrap();
        jobs.push_back(job);
        self.available.notify_one();
    }

    fn take_job(&self) -> Job {
        let mut jobs = self.jobs.lock().unwrap();
        while jobs.is_empty() {
            jobs = self.available.wait(jobs).unwrap();
        }
        jobs.pop_front().unwrap()
    }
}

fn worker(id: usize, job_queue: Arc<JobQueue>) {
    loop {
        let job = job_queue.take_job();
        println!("Worker {} got a job; executing.", id);
        job();
    }
}

fn main() {
    // pubsub
    let mut publisher = Publisher::new();
    let subscribers = Box::new(ConcreteSubscriber);

    publisher.subscribe("topic".to_string(), subscribers);

    publisher.publish("topic", &"Hello world!".to_string());

    // job queue
    let job_queue = JobQueue::new();

    let mut workers = vec![];
    for id in 0..4 {
        let job_queue_clone = job_queue.clone();
        workers.push(thread::spawn(move || {
            worker(id, job_queue_clone);
        }));
    }

    for _ in 0..10 {
        job_queue.add_job(Box::new(|| {
            println!("Running a job");
        }));
    }

    for worker in workers {
        worker.join().unwrap();
    }
}

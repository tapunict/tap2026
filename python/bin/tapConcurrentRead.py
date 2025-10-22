#!/usr/bin/env python
# From https://medium.com/@philamersune/bypassing-the-gil-a-concurrent-kafka-consumer-dcc44ae9260a
import logging
import os
import threading
import time
from collections import defaultdict
from multiprocessing import Process
from queue import Queue, Empty

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

def _process_msg(q, offsets_dict, offsets_lock, stop_event, process_time=0, process_name=""):
    """Process messages from queue and track offsets for batch commits."""
    msg_count = 0
    thread_id = threading.get_ident()
    thread_name = threading.current_thread().name
    logging.info('[%s] [%s] Worker thread started', process_name, thread_name)
    
    while not stop_event.is_set():
        try:
            msg = q.get(timeout=0.5)
            # Process message (lightweight operation)
            if msg.value:
                msg_count += 1
                try:
                    msg_str = msg.value.decode('utf-8')
                except:
                    msg_str = str(msg.value)
                
                logging.info(
                    '[%s] [%s] [Partition %d] Message #%d (offset=%d): %s',
                    process_name, thread_name, msg.partition, msg_count, msg.offset, msg_str
                )
                logging.debug(
                    '[%s] [%s] [Partition %d] Details: key=%s, value_len=%d, timestamp=%s, topic=%s',
                    process_name, thread_name, msg.partition, msg.key, len(msg.value), msg.timestamp, msg.topic
                )
            
            # Simulate processing time if configured
            if process_time > 0:
                time.sleep(process_time)
            
            q.task_done()
            
            # Track offset for batch commit (non-blocking)
            tp = TopicPartition(msg.topic, msg.partition)
            with offsets_lock:
                offsets_dict[tp] = OffsetAndMetadata(msg.offset + 1, None)
        except Empty:
            continue
        except Exception as e:
            logging.exception('[%s] [%s] Error processing message: %s', process_name, thread_name, e)
    
    logging.info('[%s] [%s] Worker thread stopping (processed %d total messages)', process_name, thread_name, msg_count)


def _consume(config):
    """Consumer process that polls messages and manages batch commits."""
    # Re-configure logging in child process
    log_level_str = os.getenv('LOGLEVEL', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logging.basicConfig(
        level=log_level,
        format='[%(asctime)s] %(levelname)s:%(message)s',
        force=True
    )
    
    process_name = f"Worker-{os.getpid()}"
    
    logging.info(
        '[%s] Starting consumer: group=%s, topic=%s, threads=%d',
        process_name, config['kafka_kwargs']['group_id'], config['topic'], 
        config['num_threads']
    )
    logging.debug('[%s] Kafka config: %s', process_name, config['kafka_kwargs'])
    
    try:
        c = KafkaConsumer(**config['kafka_kwargs'])
        logging.info('[%s] KafkaConsumer created successfully', process_name)
    except Exception as e:
        logging.error('[%s] Failed to create KafkaConsumer: %s', process_name, e)
        raise
    
    try:
        c.subscribe([config['topic']])
        logging.info('[%s] Subscribed to topic: %s', process_name, config['topic'])
        
        # Wait for partition assignment
        logging.info('[%s] Waiting for partition assignment...', process_name)
        for _ in range(10):  # Try up to 10 times
            time.sleep(0.5)
            partitions = c.assignment()
            if partitions:
                partition_list = [p.partition for p in partitions]
                logging.info('[%s] Partition assignment complete: %s', process_name, partition_list)
                break
        else:
            logging.warning('[%s] No partitions assigned after 5 seconds', process_name)
            
    except Exception as e:
        logging.error('[%s] Failed to subscribe to topic: %s', process_name, e)
        c.close()
        raise
    
    # Configuration
    queue_size = config.get('queue_size', config['num_threads'] * 2)
    q = Queue(maxsize=queue_size)
    offsets_dict = {}
    offsets_lock = threading.Lock()
    stop_event = threading.Event()
    threads = []
    
    process_time = config.get('process_time', 0)

    # Start worker threads
    for i in range(config['num_threads']):
        t = threading.Thread(
            target=_process_msg,
            args=(q, offsets_dict, offsets_lock, stop_event, process_time, process_name),
            daemon=False,
            name=f'Thread-{i}'
        )
        t.start()
        threads.append(t)
    
    logging.info('[%s] Started %d worker threads', process_name, config['num_threads'])

    # Batch commit tracking
    last_commit_time = time.time()
    commit_interval = config.get('commit_interval', 5)  # seconds
    batch_size = config.get('batch_size', 100)  # commit every N messages
    committed_count = 0
    partitions_seen = set()

    try:
        poll_count = 0
        while not stop_event.is_set():
            try:
                # Poll messages with shorter timeout for responsiveness
                messages = c.poll(timeout_ms=config.get('poll_timeout', 1000))
                poll_count += 1
                
                if messages:
                    total_msgs = sum(len(msgs) for msgs in messages.values())
                    partitions_in_batch = list(messages.keys())
                    
                    # Track which partitions have sent messages
                    for tp in partitions_in_batch:
                        partitions_seen.add(tp.partition)
                    
                    logging.info(
                        '[%s] Poll #%d: Received %d messages from %d partitions: %s',
                        process_name, poll_count, total_msgs, len(messages), 
                        [p.partition for p in partitions_in_batch]
                    )
                    for topic_partition, msgs in messages.items():
                        logging.debug(
                            '[%s] Partition %d: %d messages (topic=%s)',
                            process_name, topic_partition.partition, len(msgs), topic_partition.topic
                        )
                        for msg in msgs:
                            try:
                                q.put(msg, timeout=2)
                            except Exception as e:
                                logging.error('[%s] Failed to put message in queue: %s', process_name, e)
                else:
                    if poll_count % 10 == 0:
                        logging.debug('[%s] Poll #%d: No messages received', process_name, poll_count)
                        # Log current partition assignment
                        partitions = c.assignment()
                        if partitions:
                            partition_list = [p.partition for p in partitions]
                            logging.debug('[%s] Current partition assignment: %s', process_name, partition_list)
                
                # Check if we should commit
                current_time = time.time()
                should_commit = False
                
                with offsets_lock:
                    if len(offsets_dict) > 0:
                        if (len(offsets_dict) >= batch_size or 
                            current_time - last_commit_time >= commit_interval):
                            should_commit = True
                
                if should_commit:
                    with offsets_lock:
                        if offsets_dict:
                            try:
                                c.commit(offsets=offsets_dict)
                                committed_count += len(offsets_dict)
                                offsets_info = '; '.join([f'Partition {tp.partition}: offset {om.offset}' for tp, om in offsets_dict.items()])
                                logging.info(
                                    '[%s] Batch committed %d offsets (total: %d) [%s]',
                                    process_name, len(offsets_dict), committed_count, offsets_info
                                )
                                offsets_dict.clear()
                                last_commit_time = current_time
                            except Exception as e:
                                logging.error('[%s] Commit failed: %s', process_name, e)
                                
            except Exception as e:
                logging.exception('[%s] Error polling messages: %s', process_name, e)
                break
                
    except KeyboardInterrupt:
        logging.info('[%s] Consumer interrupted', process_name)
    finally:
        partitions_assigned = [p.partition for p in c.assignment()]
        logging.info(
            '[%s] Shutting down consumer (polls=%d, total_commits=%d, remaining_offsets=%d, partitions_assigned=%s, partitions_seen=%s)',
            process_name, poll_count if 'poll_count' in locals() else 0, committed_count, len(offsets_dict),
            partitions_assigned, sorted(list(partitions_seen)) if 'partitions_seen' in locals() else []
        )
        stop_event.set()
        logging.info('[%s] Waiting for queue to drain...', process_name)
        q.join()
        logging.info('[%s] Queue drained', process_name)
        
        # Final commit of remaining offsets
        with offsets_lock:
            if offsets_dict:
                try:
                    c.commit(offsets=offsets_dict)
                    offsets_info = '; '.join([f'Partition {tp.partition}: offset {om.offset}' for tp, om in offsets_dict.items()])
                    logging.info('[%s] Final commit: %d offsets [%s]', process_name, len(offsets_dict), offsets_info)
                except Exception as e:
                    logging.error('[%s] Final commit failed: %s', process_name, e)
        
        # Wait for all threads to finish
        logging.info('[%s] Waiting for %d worker threads to finish...', process_name, len(threads))
        for i, t in enumerate(threads):
            t.join(timeout=3)
            if t.is_alive():
                logging.warning('[%s] %s still alive after timeout', process_name, t.name)
            else:
                logging.debug('[%s] %s finished', process_name, t.name)
        
        c.close()
        logging.info('[%s] Consumer closed', process_name)


def main(config):
    """
    Concurrent Kafka consumer with batch offset commits.
    
    Configurable parameters:
    - num_workers: Number of worker processes
    - num_threads: Number of consumer threads per process
    - queue_size: Size of message queue (default: num_threads * 2)
    - batch_size: Commit after N messages (default: 100)
    - commit_interval: Commit after N seconds (default: 5)
    - poll_timeout: Kafka poll timeout in ms (default: 1000)
    - process_time: Simulated processing time per message (default: 0)
    """
    workers = []
    try:
        logging.info('Main: Starting %d worker processes...', config['num_workers'])
        while True:
            num_alive = len([w for w in workers if w.is_alive()])
            if config['num_workers'] == num_alive:
                time.sleep(1)
                continue
            for _ in range(config['num_workers'] - num_alive):
                p = Process(target=_consume, daemon=False, args=(config,))
                p.start()
                workers.append(p)
                logging.info('Main: Started worker process #%s (alive: %d/%d)', p.pid, len([w for w in workers if w.is_alive()]), config['num_workers'])
            time.sleep(0.5)
    except KeyboardInterrupt:
        logging.info('Main interrupted. Terminating workers...')
        for p in workers:
            if p.is_alive():
                p.terminate()
                p.join(timeout=5)
        logging.info('All workers terminated.')


if __name__ == '__main__':
    # Get log level from environment variable (default: INFO)
    log_level_str = os.getenv('LOGLEVEL', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    logging.basicConfig(
        level=log_level,
        format='[%(asctime)s] %(levelname)s:%(name)s:%(message)s',
    )
    logging.info('Starting TAP Concurrent Kafka Reader (LOGLEVEL=%s)', log_level_str)
    logging.info('Python version: %s', os.sys.version)
    logging.info('Working directory: %s', os.getcwd())
    
    main(config={
        'num_workers': 2,
        'num_threads': 8,           # Increased for better throughput
        'queue_size': 32,           # Buffer for smoother processing
        'batch_size': 100,          # Commit every 100 messages
        'commit_interval': 5,       # Commit every 5 seconds
        'poll_timeout': 1000,       # Poll every 1 second
        'process_time': 0,          # Set > 0 to simulate processing time
        'topic': 'tap',
        'kafka_kwargs': {
            'bootstrap_servers': 'kafkaServer:9092',
            'group_id': 'my_consumer_group',
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'max_poll_records': 500,  # Fetch more records per poll
            'fetch_min_bytes': 1024,  # Wait for more data before returning
            'fetch_max_wait_ms': 100,  # But don't wait too long
        },
    })
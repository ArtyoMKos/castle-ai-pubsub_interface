# -*- coding: utf-8 -*-
# @Time    : 22.06.2024
# @Author  : Artyom Kosakyan
# @Email   : gevorg@podcastle.ai
"""
Module supposed to be moved to the separate package as google pubsub interface for Podcastle AI team
"""
import json
import logging
import traceback
import threading
from typing import List, Callable
from concurrent.futures import ThreadPoolExecutor
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.scheduler import ThreadScheduler
from google.api_core.exceptions import DeadlineExceeded
from google.api_core import retry
from google.pubsub_v1 import PullRequest

logger = logging.getLogger("castle_se_service")


# pylint: disable=[too-many-arguments, too-many-instance-attributes]
class GooglePubSubSubscriber(threading.Thread):
    def __init__(
        self,
        project_id: str,
        *,
        subscription: str,
        max_messages: int,
        max_lease_duration: int,
        subscriber: pubsub_v1.SubscriberClient,
        unhealthy_file_path: str,
        message_processor: Callable,
        max_workers: int = 8,
        consume_timeout: int = 60,
    ):
        threading.Thread.__init__(self)
        self.message_processor = message_processor
        self.subscriber = subscriber
        self.subscription_path = subscriber.subscription_path(project_id, subscription)
        self._subscription = subscription
        self.running = True
        self.max_workers = max_workers
        self.unhealthy_file_path = unhealthy_file_path
        self.consume_timeout = consume_timeout
        self.flow_control = pubsub_v1.types.FlowControl(
            max_messages=max_messages, max_lease_duration=max_lease_duration
        )
        self.pull_request = PullRequest(
            subscription=self.subscription_path,
            max_messages=max_messages,
        )
        self.streaming_pull_future = None

    def pull_messages(self) -> dict:
        """
        Pull messages from Pub/Sub. Count of messages defined in general configs
        :return Dict[Dict]
        """
        try:
            response = self.subscriber.pull(
                request=self.pull_request,
                return_immediately=False,
                retry=retry.Retry(deadline=300),
                timeout=self.consume_timeout,
            )
        except DeadlineExceeded:
            return {}
        return {
            received_message.ack_id: json.loads(received_message.message.data)
            for received_message in response.received_messages
        }

    def acknowledge_cumulative(self, ack_ids: List[str]) -> None:
        """
        Acknowledge multiple messages at same time
        """
        self.subscriber.acknowledge(
            request={"subscription": self.subscription_path, "ack_ids": ack_ids}
        )

    @staticmethod
    def _make_scheduler(max_workers: int) -> ThreadScheduler:
        executor = ThreadPoolExecutor(max_workers=max_workers)
        return ThreadScheduler(executor=executor)

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        """
        Triggering function if message passed
        """
        try:
            message_dict = json.loads(message.data.decode("utf-8"))
            self.message_processor(message_dict)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(
                f"Fail to process message: {message.data}, Error: {str(e)} %s",
                traceback.format_exc(),
            )
        message.ack()

    def subscribe(self):
        """
        Create google pubsub subscription
        """
        scheduler = self._make_scheduler(max_workers=self.max_workers)
        self.streaming_pull_future = self.subscriber.subscribe(
            self.subscription_path,
            callback=self.callback,
            flow_control=self.flow_control,
            scheduler=scheduler,
            await_callbacks_on_shutdown=True,
        )

    def run(self):
        with self.subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                self.streaming_pull_future.result()
            except TimeoutError:
                self.streaming_pull_future.cancel()
                self.streaming_pull_future.result()
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.exception(f"error occurred in pubsub worker: {str(e)}")
                self.streaming_pull_future.cancel()  # Trigger the shutdown.
                self.streaming_pull_future.result()  # Block until the shutdown is complete.
                with open(
                    self.unhealthy_file_path, "w"
                ) as file:  # pylint: disable=[unused-variable,unspecified-encoding]
                    pass

    def stop(self):
        """
        Stopping consumer subscription
        """
        logger.info(f"stopping {self._subscription}")
        self.streaming_pull_future.cancel()
        self.streaming_pull_future.result(timeout=600)
        self.subscriber.close()
        self.running = False
        logger.info(f"{self._subscription} stopped")


class SubscriberDispatcher:

    def __init__(
        self,
        subscribers: List[GooglePubSubSubscriber],
    ):
        self.subscribers = subscribers

    def subscribe_all(self) -> List[GooglePubSubSubscriber]:
        """
        Starting subscriber threads
        """
        for it in self.subscribers:
            it.start()
        return self.subscribers

    def stop(self):
        """
        Stopping subscriber threads.
        """
        logger.info("subscribers stopping...")
        for it in self.subscribers:
            it.stop()

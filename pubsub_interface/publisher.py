import json
from concurrent.futures import ThreadPoolExecutor
from google import api_core
from google.cloud.pubsub_v1 import PublisherClient


class GoogleMessagePublisher:

    def __init__(
        self,
        project_id: str,
    ):
        self._project_id = project_id
        self._client = PublisherClient()

    def publish_message(self, message: dict, topic_id: str):
        """
        Publish single message to Pub/Sub topic
        """
        custom_retry = api_core.retry.Retry(
            initial=0.1,
            maximum=10.0,
            multiplier=1.3,
            deadline=10.0,
            predicate=api_core.retry.if_exception_type(
                api_core.exceptions.Aborted,
                api_core.exceptions.DeadlineExceeded,
                api_core.exceptions.InternalServerError,
                api_core.exceptions.ResourceExhausted,
                api_core.exceptions.ServiceUnavailable,
                api_core.exceptions.Unknown,
                api_core.exceptions.Cancelled,
            ),
        )
        topic_path = self._client.topic_path(self.project_id, topic_id)
        data = json.dumps(message).encode("utf-8")
        future = self._client.publish(topic_path, data, retry=custom_retry)
        future.result()

    @property
    def project_id(self):
        return self._project_id

    def batch_publish_messages(
        self, messages: list, topic_id: str, pool_size: int = 8
    ) -> None:
        """
        Publishing batch of messages
        """
        # Create a ThreadPoolExecutor with the specified pool_size
        with ThreadPoolExecutor(max_workers=pool_size) as executor:
            # Use the map function to submit tasks to the executor in parallel
            # The publish_message function is applied to each message in the messages list
            def publish(message):
                return self.publish_message(message, topic_id)

            executor.map(publish, messages)

    async def batch_publish_messages_async(
        self, messages, topic_id, current_loop, pool_size=8
    ):
        """
        Publishing batch of messages asynchronously
        """
        pass

<?php

declare(strict_types=1);

namespace AsyncAws\SqsExtended;

use AsyncAws\Core\Exception\InvalidArgument;
use AsyncAws\S3\S3Client;
use AsyncAws\Sqs\Input\ReceiveMessageRequest;
use AsyncAws\Sqs\Input\SendMessageRequest;
use AsyncAws\Sqs\Result\ReceiveMessageResult;
use AsyncAws\Sqs\Result\SendMessageResult;
use AsyncAws\Sqs\SqsClient;
use AsyncAws\Sqs\ValueObject\Message;
use AsyncAws\SQSExtended\PayloadOffloading\PayloadStore;

/**
 * A simplified S3 client that hides some of the complexity of working with S3.
 * The aim of this client is to provide shortcut methods to about 80% common tasks.
 *
 * @author Jérôme Tamarelle <jerome@tamarelle.net>
 */
class SqsExtendedClient extends SqsClient
{
    private const RESERVED_ATTRIBUTE_NAME = "ExtendedPayloadSize";
    private const DEFAULT_MESSAGE_SIZE_THRESHOLD = 256*1024; // 256KB
    private const MAX_ALLOWED_ATTRIBUTES = 10-1; // 10 for SQS, 1 for the reserved attribute
    private const S3_BUCKET_NAME_MARKER = "-..s3BucketName..-";
    private const S3_KEY_MARKER = "-..s3Key..-";

    /**
     * @var PayloadStore
     */
    private $payloadStore;

    /**
     * @var bool
     */
    private $cleanupPayload;

    /**
     * Enables support for payload messages.
     * @param PayloadStore $payloadStore
     * @param bool $cleanupPayload
     *            If set to true, would handle deleting the S3 object as part
     *            of deleting the message from SQS queue. Otherwise, would not
     *            attempt to delete the object from S3. If opted to not delete S3
     *            objects its the responsibility to the message producer to handle
     *            the clean up appropriately.
     */
    public function setPayloadSupportEnabled(PayloadStore $payloadStore, bool $cleanupPayload = true): void
    {
        $this->payloadStore = $payloadStore;
        $this->cleanupPayload = $cleanupPayload;
    }

    public function isPayloadSupportEnabled(): bool
    {
        return isset($this->payloadStore);
    }

    public function sendMessage($input): SendMessageResult
    {
        if (!$this->isPayloadSupportEnabled()) {
            return parent::sendMessage($input);
        }

        $input = SendMessageRequest::create($input);

        $this->checkMessageAttributes($input->getMessageAttributes());

        if ($this->isLarge($input)) {
            $input = $this->storeMessageInS3($input);
        }

        return parent::sendMessage($input);
    }

    public function receiveMessage($input): ReceiveMessageResult
    {
        if (!$this->isPayloadSupportEnabled()) {
            return parent::receiveMessage($input);
        }

        $input = ReceiveMessageRequest::create($input);
        $attributes = $input->getAttributeNames();
        if (!in_array(self::RESERVED_ATTRIBUTE_NAME, $attributes)) {
            $attributes[] = self::RESERVED_ATTRIBUTE_NAME;
        }
        $input->setAttributeNames($attributes);

        $result = parent::receiveMessage($input);

        // for each received message check if they are stored in S3.
        foreach ($result->getMessages() as &$message) {
            $attributes = $message->getMessageAttributes();
            if (array_key_exists(self::RESERVED_ATTRIBUTE_NAME, $attributes)) {
                unset($attributes[self::RESERVED_ATTRIBUTE_NAME]);
                $pointer = $this->payloadStore->getPointerFromString($message->getBody());

                // @todo async & parallel download
                $body = $this->payloadStore->getOriginalPayload($pointer);

                $message = new Message([
                    'MessageId' => $message->getMessageId(),
                    'ReceiptHandle' => $message->getReceiptHandle(),
                    'MD5OfBody' => null,
                    'Body' => $body,
                    'Attributes' => $message->getAttributes(),
                    'MD5OfMessageAttributes' => null,
                    'MessageAttributes' => $attributes,
                ]);

                // @todo ReceiptHandle
            }
        }

        return $result;
    }

    /**
     * Check message attributes for ExtendedClient related constraints.
     * @param array $attributes
     */
    private function checkMessageAttributes(array $attributes): void
    {
        $messageAttributesSize = $this->getMessageAttributesSize($attributes);
        if ($messageAttributesSize > self::DEFAULT_MESSAGE_SIZE_THRESHOLD) {
            throw new InvalidArgument('Total size of Message attributes is ' . $messageAttributesSize . ' bytes which is larger than the threshold of ' . self::DEFAULT_MESSAGE_SIZE_THRESHOLD . ' Bytes. Consider including the payload in the message body instead of message attributes.');
        }

        $messageAttributesCount = count($attributes);
        if ($messageAttributesCount > self::MAX_ALLOWED_ATTRIBUTES) {
            throw new InvalidArgument('Number of message attributes [' . $messageAttributesCount . '] exceeds the maximum allowed for large-payload messages [' . self::MAX_ALLOWED_ATTRIBUTES . ']');
        }

        if (array_key_exists(self::RESERVED_ATTRIBUTE_NAME, $attributes)) {
            throw new InvalidArgument('Message attribute name ' . self::RESERVED_ATTRIBUTE_NAME . ' is reserved for use by SQS extended client.');
        }
    }

    /**
     * @param array $attributes
     * @return int
     */
    private function getMessageAttributesSize(array $attributes): int
    {
        $size = 0;

        foreach ($attributes as $name => $value) {
            $size += strlen($name);
            $size += strlen($value);
        }

        return $size;
    }

    private function isLarge(SendMessageRequest $request): bool
    {
        $msgAttributesSize = $this->getMessageAttributesSize($request->getMessageAttributes());
        $msgBodySize = strlen($request->getMessageBody());

        return ($msgAttributesSize + $msgBodySize) > self::DEFAULT_MESSAGE_SIZE_THRESHOLD;
    }

    private function storeMessageInS3(SendMessageRequest $request): SendMessageRequest
    {
        // @todo $request should be cloned?
        $messageContent = $request->getMessageBody();
        $messageContentSize = strlen($messageContent);

        $attributes = $request->getMessageAttributes();
        $attributes[self::RESERVED_ATTRIBUTE_NAME] = $messageContentSize;
        $request->setMessageAttributes();

        $largeMessagePointer = $this->payloadStore->storeOriginalPayload($request->getMessageBody());

        $request->setMessageBody($largeMessagePointer->asString());

        return $request;
    }
}

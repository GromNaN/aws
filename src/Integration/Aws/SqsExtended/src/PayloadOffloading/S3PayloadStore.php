<?php

namespace AsyncAws\SQSExtended\PayloadOffloading;

use AsyncAws\Core\Exception\LogicException;
use AsyncAws\S3\S3Client;
use Symfony\Component\Uid\Uuid;

class S3PayloadStore implements PayloadStore
{
    /**
     * @var S3Client
     */
    private $s3;

    /**
     * @var string;
     */
    private $s3BucketName;

    /**
     * @param S3Client $s3
     *            Amazon S3 client which is going to be used for storing
     *            payload messages.
     * @param string $s3BucketName
     *            Name of the bucket which is going to be used for storing
     *            payload messages. The bucket must be already created and
     *            configured in s3.
     */
    public function __construct(S3Client $s3, string $s3Bucket)
    {
        $this->s3 = $s3;
        $this->s3BucketName = $s3Bucket;
    }

    /**
     * {@inheritDoc}
     */
    public function storeOriginalPayload(string $payload, string $key = null): PayladPointer
    {
        if (null === $key) {
            if (!class_exists(Uuid::class)) {
                throw new LogicException('Package symfony/uid is required to generate random payload key.');
            }

            $key = Uuid::v1();
        }

        $this->s3->putObject([
            'Bucket' => $this->s3BucketName,
            'Key' => $key,
            'Body' => $payload,
        ])->resolve();

        // @todo async?
        // @todo ACL

        return new S3PayloadPointer($this->s3BucketName, $key);
    }

    /**
     * {@inheritDoc}
     */
    public function getOriginalPayload(PayloadPointer $pointer): string
    {
        if (!$pointer instanceof S3PayloadPointer) {
            throw new LogicException(__FUNCTION__.' supports only '.S3PayloadPointer::class.'. Instance of '.get_class($pointer).' received.');
        }

        return $this->s3->getObject([
            'Bucket' => $pointer->getS3BucketName(),
            'Key' => $pointer->getS3Key(),
        ])->getBody()->getContentAsString();
    }

    /**
     * {@inheritDoc}
     */
    public function deleteOriginalPayload(PayloadPointer $pointer): void
    {
        if (!$pointer instanceof S3PayloadPointer) {
            throw new LogicException(__FUNCTION__.' supports only '.S3PayloadPointer::class.'. Instance of '.get_class($pointer).' received.');
        }

        $this->s3->deleteObject([
            'Bucket' => $pointer->getS3BucketName(),
            'Key' => $pointer->getS3Key(),
        ])->resolve();
    }
}

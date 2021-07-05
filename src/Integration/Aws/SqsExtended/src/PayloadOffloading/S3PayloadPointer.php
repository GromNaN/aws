<?php

namespace AsyncAws\SQSExtended\PayloadOffloading;

use AsyncAws\Core\Exception\RuntimeException;

class S3PayloadPointer implements PayloadPointer
{
    private $s3BucketName;
    private $s3Key;

    public function __construct(string $s3Bucket, string $s3Key)
    {
        $this->s3BucketName = $s3Bucket;
        $this->s3Key = $s3Key;
    }

    public function getS3BucketName(): string
    {
        return $this->s3BucketName;
    }

    public function getS3Key(): string
    {
        return $this->s3Key;
    }

    public function asString(): string
    {
        return json_encode($this);
    }

    public static function fromString(string $json): self
    {
        try {
            $data = \json_decode($json, false, 2, JSON_THROW_ON_ERROR);

            if (!isset($data->s3BucketName) || !isset($data->s3Key)) {
                throw new RuntimeException('Invalid S3 Payload Pointer');
            }

            return new self($data->s3BucketName, $data->s3Key);
        } catch (\JsonException $e) {
            throw new RuntimeException('Invalid S3 Payload Pointer', 0, $e);
        }
    }
}

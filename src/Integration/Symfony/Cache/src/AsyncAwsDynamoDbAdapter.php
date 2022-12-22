<?php

namespace AsyncAws\Illuminate\Cache;

use AsyncAws\Core\Exception\Http\HttpException;
use AsyncAws\Core\Exception\RuntimeException;
use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\Enum\KeyType;
use AsyncAws\DynamoDb\Exception\ConditionalCheckFailedException;
use AsyncAws\DynamoDb\ValueObject\KeySchemaElement;
use Illuminate\Contracts\Cache\LockProvider;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Support\Carbon;
use Illuminate\Support\InteractsWithTime;
use Symfony\Component\Cache\Adapter\AbstractAdapter;
use Symfony\Component\Cache\Marshaller\DefaultMarshaller;
use Symfony\Component\Cache\Marshaller\MarshallerInterface;

/**
 * Adapater for Symfony Cache
 */
class AsyncAwsDynamoDbAdapter extends AbstractAdapter
{
    /**
     * The DynamoDB client instance.
     *
     * @var DynamoDbClient
     */
    private $dynamoDb;

    /**
     * The table name.
     *
     * @var string
     */
    private $table;

    /**
     * The name of the attribute that should hold the key.
     *
     * @var string
     */
    private $keyAttribute;

    /**
     * The name of the attribute that should hold the value.
     *
     * @var string
     */
    private $valueAttribute;

    /**
     * The name of the attribute that should hold the expiration timestamp.
     *
     * @var string
     */
    private $expirationAttribute;

    /**
     * @var MarshallerInterface
     */
    private $marshaller;

    /**
     * Create a new store instance.
     *
     * @param string $table
     * @param string $keyAttribute
     * @param string $valueAttribute
     * @param string $expirationAttribute
     * @param string $namespace
     * @param int    $defaultLifetime
     *
     * @return void
     */
    public function __construct(
        DynamoDbClient $dynamoDb,
        $table,
        $keyAttribute = 'key',
        $valueAttribute = 'value',
        $expirationAttribute = 'expires_at',
        $namespace = '',
        $defaultLifetime = 0,
        MarshallerInterface $marshaller = null
    ) {
        $this->table = $table;
        $this->dynamoDb = $dynamoDb;
        $this->keyAttribute = $keyAttribute;
        $this->valueAttribute = $valueAttribute;
        $this->expirationAttribute = $expirationAttribute;
        $this->marshaller = $marshaller ?? new DefaultMarshaller();

        parent::__construct($namespace, $defaultLifetime);
    }

    protected function doFetch(array $ids): iterable
    {
        $results = [];
        foreach ($ids as $id) {
            $results[$id] = $this->dynamoDb->getItem([
                'TableName' => $this->table,
                'ConsistentRead' => false,
                'Key' => [
                    $this->keyAttribute => [
                        'S' => $id,
                    ],
                ],
            ]);
        }
        foreach ($results as $id => $result) {
            $item = $result->getItem();
            if ($item && isset($item[$this->valueAttribute])) {
                $results[$id] = $this->marshaller->unmarshall(
                    $item[$this->valueAttribute]->getS() ??
                    $item[$this->valueAttribute]->getN() ??
                    null
                );
            } else {
                unset($results[$id]);
            }
        }

        return $results;
    }

    protected function doHave(string $id): bool
    {
        // TODO: Implement doHave() method.
    }

    protected function doClear(string $namespace): bool
    {
        // TODO: Implement doClear() method.
    }

    protected function doDelete(array $ids): bool
    {
        $success = true;
        $results = [];
        foreach ($ids as $id) {
            $results[$id] = $this->dynamoDb->deleteItem([
                'TableName' => $this->table,
                'Key' => [
                    $this->keyAttribute => [
                        'S' => $id,
                    ],
                ],
            ]);
        }
        foreach ($results as $id => $result) {
            try {
                $result->resolve();
            } catch (HttpException $e) {
                $success = false;
            }
        }

        return $success;
    }

    protected function doSave(array $values, int $lifetime): array
    {
        $results = [];
        foreach ($values as $id => $value) {
            $this->dynamoDb->putItem([
                'TableName' => $this->table,
                'Item' => [
                    $this->keyAttribute => [
                        'S' => $this->prefix . $key,
                    ],
                    $this->valueAttribute => [
                        $this->type($value) => $this->serialize($value),
                    ],
                    $this->expirationAttribute => [
                        'N' => (new \DateTimeImmutable())->add($lifetime.'s')->format('U'),
                    ],
                ],
            ]);
        }

        return true;
    }
}

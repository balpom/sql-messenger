<?php

declare(strict_types=1);

namespace Balpom\Messenger\Bridge\Sql\Transport;

use Balpom\Messenger\Bridge\Sql\Exception\TableNotFoundException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Contracts\Service\ResetInterface;
use \Throwable;
use \DateTimeImmutable;
use \PDO;
use \PDOException;

class SqlConnection implements ResetInterface
{
    protected const DEFAULT_OPTIONS = [
        'table_name' => 'messenger_messages',
        'queue_name' => 'default',
        'redeliver_timeout' => 3600,
        'auto_setup' => true,
    ];
    private PDO $pdo;
    protected array $configuration;
    private bool $autoSetup;
    protected ?float $queueEmptiedAt = null;

    public function __construct(PDO $pdo, array $configuration)
    {
        $this->configuration = array_replace_recursive(static::DEFAULT_OPTIONS, $configuration);
        $this->pdo = $pdo;
        $this->autoSetup = $this->configuration['auto_setup'];
        $this->init();
    }

    public function reset(): void
    {
        $this->queueEmptiedAt = null;
    }

    /**
     * @param int $delay The delay in milliseconds
     *
     * @return string The inserted id
     */
    public function send(string $body, array $headers, int $delay = 0): string
    {
        $headers = json_encode($headers);
        $tableName = $this->configuration['table_name'];
        $queueName = $this->configuration['queue_name'];
        $now = new DateTimeImmutable('UTC');
        $availableAt = $now->modify(sprintf('%+d seconds', $delay / 1000));
        $now = $now->format('U.u');
        $availableAt = $availableAt->format('U.u');

        $sq = "'";
        $query = 'INSERT INTO ' . $tableName . '(body, headers, queue_name, created_at, available_at)
                  VALUES(' . $sq . $body . $sq . ', "' . $headers . '", "' . $queueName . '", "' . $now . '", "' . $availableAt . '")
                  returning id';

        if (!$stmt = $this->pdo->query($query)) {
            return false;
        }

        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        return (string) $row['id'];
    }

    public function get(): ?array
    {
        get:
        //$this->pdo->beginTransaction();
        $this->pdo->query('BEGIN IMMEDIATE TRANSACTION'); // https://habr.com/ru/articles/204438/
        try {
            $tableName = $this->configuration['table_name'];
            $queueName = $this->configuration['queue_name'];
            $now = new DateTimeImmutable('UTC');
            $redeliverLimit = $now->modify(sprintf('-%d seconds', $this->configuration['redeliver_timeout']));
            $now = $now->format('U.u');
            $redeliverLimit = $redeliverLimit->format('U.u');

            $query = 'SELECT m.* FROM ' . $tableName . ' m WHERE (m.queue_name = "' . $queueName . '")
                      AND (m.delivered_at is null OR m.delivered_at < ' . $redeliverLimit . ')
                      AND (m.available_at <= ' . $now . ') ORDER BY available_at ASC LIMIT 1';

            $stmt = $this->pdo->query($query);
            $envelope = $stmt->fetch();

            if (false === $envelope) {
                //$this->pdo->commit();
                $this->pdo->query('COMMIT TRANSACTION');
                $this->queueEmptiedAt = microtime(true) * 1000;

                return null;
            }

            $this->queueEmptiedAt = null;
            $envelope['id'] = (string) $envelope['id'];
            $envelope = $this->decodeEnvelopeHeaders($envelope);

            $now = new DateTimeImmutable('UTC');
            $now = $now->format('U.u');

            $query = "UPDATE messenger_messages SET delivered_at = " . $now . " WHERE id = '" . $envelope['id'] . "'";

            $stmt = $this->pdo->query($query);
            //$this->pdo->commit();
            $this->pdo->query('COMMIT TRANSACTION');

            return $envelope;
        } catch (Throwable $e) {

            echo $e;
            die;

            //$this->pdo->rollBack();
            $this->pdo->query('ROLLBACK TRANSACTION');

            if ($this->autoSetup && $e instanceof TableNotFoundException) {
                $this->setup();
                goto get;
            }

            throw $e;
        }
    }

    public function ack(string $id): bool
    {
        $tableName = $this->configuration['table_name'];
        try {
            $query = 'DELETE FROM ' . $tableName . ' WHERE id = "' . $id . '"';
            $stmt = $this->pdo->query($query);
            return $stmt->fetch() ? true : false;
        } catch (PDOException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function reject(string $id): bool
    {
        $tableName = $this->configuration['table_name'];
        try {
            $query = 'DELETE FROM ' . $tableName . ' WHERE id = "' . $id . '"';
            $stmt = $this->pdo->query($query);
            return $stmt->fetch() ? true : false;
        } catch (PDOException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function getMessageCount(): int
    {
        $tableName = $this->configuration['table_name'];
        $queueName = $this->configuration['queue_name'];
        $now = new DateTimeImmutable('UTC');
        $redeliverLimit = $now->modify(sprintf('-%d seconds', $this->configuration['redeliver_timeout']));
        $now = $now->format('U.u');
        $redeliverLimit = $redeliverLimit->format('U.u');

        $query = 'SELECT COUNT(m.id) AS message_count FROM ' . $tableName . ' m WHERE (m.queue_name = "' . $queueName . '")
                  AND (m.delivered_at is null OR m.delivered_at < ' . $redeliverLimit . ')
                  AND (m.available_at <= ' . $now . ') LIMIT 1';

        $stmt = $this->pdo->query($query);

        return $stmt->fetchColumn();
    }

    public function findAll(?int $limit = null): array
    {
        $tableName = $this->configuration['table_name'];
        $queueName = $this->configuration['queue_name'];
        $now = new DateTimeImmutable('UTC');
        $redeliverLimit = $now->modify(sprintf('-%d seconds', $this->configuration['redeliver_timeout']));
        $now = $now->format('U.u');
        $redeliverLimit = $redeliverLimit->format('U.u');

        $query = 'SELECT m.* FROM ' . $tableName . ' m WHERE (m.queue_name = "' . $queueName . '")
                  AND (m.delivered_at is null OR m.delivered_at < ' . $redeliverLimit . ')
                  AND (m.available_at <= ' . $now . ')';
        if (null !== $limit) {
            $query .= ' LIMIT ' . $limit;
        }

        $stmt = $this->pdo->query($query);
        $data = $stmt->fetchAll();

        return array_map(fn($doctrineEnvelope) => $this->decodeEnvelopeHeaders($doctrineEnvelope), $data);
    }

    public function find(mixed $id): ?array
    {
        $tableName = $this->configuration['table_name'];
        $queueName = $this->configuration['queue_name'];

        $where = "m.id = '" . $id . "'";
        $query = 'SELECT m.* FROM ' . $tableName . ' m WHERE (' . $where . ' AND m.queue_name = "' . $queueName . '")';

        $stmt = $this->pdo->query($query);
        $data = $stmt->fetch();

        return false === $data ? null : $this->decodeEnvelopeHeaders($data);
    }

    private function init(): void
    {
        if (!$this->isTableExists($this->configuration['table_name'])) {
            $this->createTable($this->configuration['table_name']);
        }
    }

    private function setup(): void
    {
        $this->createTable($this->configuration['table_name']);
    }

    private function createTable(string $table): void
    {
        $query = 'CREATE TABLE IF NOT EXISTS ' . $table . ' (
                  id INTEGER PRIMARY KEY NOT NULL,
                  body TEXT NOT NULL,
                  headers TEXT NOT NULL,
                  queue_name TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  available_at TEXT NOT NULL,
                  delivered_at TEXT
                  )';
        $this->pdo->exec($query);
        $query = 'CREATE INDEX IF NOT EXISTS IDX_queue_name ON ' . $table . ' (queue_name)';
        $this->pdo->exec($query);
        $query = 'CREATE INDEX IF NOT EXISTS IDX_available_at ON ' . $table . ' (available_at)';
        $this->pdo->exec($query);
        $query = 'CREATE INDEX IF NOT EXISTS IDX_delivered_at ON ' . $table . ' (delivered_at)';
        $this->pdo->exec($query);
    }

    private function isTableExists(string $table): bool
    {
        $stmt = $this->pdo->prepare("pragma table_list('" . $table . "')");
        $stmt->execute();

        return $stmt->fetch() ? true : false;
    }

    private function decodeEnvelopeHeaders(array $envelope): array
    {
        $envelope['headers'] = json_decode($envelope['headers'], true);

        return $envelope;
    }
}

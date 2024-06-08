<?php

declare(strict_types=1);

namespace Balpom\Messenger\Bridge\Sql\Transport;

use Balpom\Messenger\Bridge\Sql\Exception\RetryableException;
use Balpom\Messenger\Bridge\Sql\Exception\SqlExceptionInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use \Exception;

class SqlReceiver implements ListableReceiverInterface, MessageCountAwareInterface
{
    private const MAX_RETRIES = 3;
    private int $retryingSafetyCounter = 0;
    private SqlConnection $connection;
    private SerializerInterface $serializer;

    public function __construct(SqlConnection $connection, ?SerializerInterface $serializer = null)
    {
        $this->connection = $connection;
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    public function get(): iterable
    {
        try {
            $envelope = $this->connection->get();
            $this->retryingSafetyCounter = 0; // reset counter
        } catch (RetryableException $exception) {
            if (++$this->retryingSafetyCounter >= self::MAX_RETRIES) {
                $this->retryingSafetyCounter = 0; // reset counter
                throw new TransportException($exception->getMessage(), 0, $exception);
            }

            return [];
        } catch (SqlExceptionInterface $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        if (null === $envelope) {
            return [];
        }

        return [$this->createEnvelopeFromData($envelope)];
    }

    public function ack(Envelope $envelope): void
    {
        $this->withRetryableExceptionRetry(function () use ($envelope) {
            $this->connection->ack($this->findSqlReceivedStamp($envelope)->getId());
        });
    }

    public function reject(Envelope $envelope): void
    {
        $this->withRetryableExceptionRetry(function () use ($envelope) {
            $this->connection->reject($this->findSqlReceivedStamp($envelope)->getId());
        });
    }

    public function getMessageCount(): int
    {
        try {
            return $this->connection->getMessageCount();
        } catch (SqlExceptionInterface $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function all(?int $limit = null): iterable
    {
        try {
            $envelopes = $this->connection->findAll($limit);
        } catch (SqlExceptionInterface $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        foreach ($envelopes as $envelope) {
            yield $this->createEnvelopeFromData($envelope);
        }
    }

    public function find(mixed $id): ?Envelope
    {
        try {
            $envelope = $this->connection->find($id);
        } catch (SqlExceptionInterface $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }

        if (null === $envelope) {
            return null;
        }

        return $this->createEnvelopeFromData($envelope);
    }

    private function findSqlReceivedStamp(Envelope $envelope): SqlReceivedStamp
    {
        $receivedStamp = $envelope->last(SqlReceivedStamp::class);

        if (null === $receivedStamp) {
            throw new LogicException('No SqlReceivedStamp found on the Envelope.');
        }

        return $receivedStamp;
    }

    private function createEnvelopeFromData(array $data): Envelope
    {
        try {
            $envelope = $this->serializer->decode([
                'body' => $data['body'],
                'headers' => $data['headers'],
            ]);
        } catch (MessageDecodingFailedException $exception) {
            $this->connection->reject($data['id']);

            throw $exception;
        }

        return $envelope->with(
                        new SqlReceivedStamp($data['id']),
                        new TransportMessageIdStamp($data['id'])
        );
    }

    private function withRetryableExceptionRetry(callable $callable): void
    {
        $delay = 100;
        $multiplier = 2;
        $jitter = 0.1;
        $retries = 0;

        retry:
        try {
            $callable();
        } catch (RetryableException $exception) {
            if (++$retries <= self::MAX_RETRIES) {
                $delay *= $multiplier;

                $randomness = (int) ($delay * $jitter);
                $delay += random_int(-$randomness, +$randomness);

                usleep($delay * 1000);

                goto retry;
            }

            throw new TransportException($exception->getMessage(), 0, $exception);
        } catch (Exception $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }
}

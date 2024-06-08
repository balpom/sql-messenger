<?php

declare(strict_types=1);

namespace Balpom\Messenger\Bridge\Sql\Transport;

use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class SqlTransportFactory implements TransportFactoryInterface
{

    public function createTransport(SqlConnection $connection, SerializerInterface $serializer): TransportInterface
    {
        return new SqlTransport($connection, $serializer);
    }

    public function supports(string $dsn, array $options): bool
    {
        return 0 === strpos($dsn, 'sqlite://');
    }
}

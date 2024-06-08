<?php

declare(strict_types=1);

namespace Balpom\Messenger\Bridge\Sql\Transport;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

class SqlReceivedStamp implements NonSendableStampInterface
{
    private string $id;

    public function __construct(string $id)
    {
        $this->id = $id;
    }

    public function getId(): string
    {
        return $this->id;
    }
}

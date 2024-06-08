<?php

declare(strict_types=1);

namespace Balpom\Messenger\Bridge\Sql\PDO;

use \PDO;

interface CreatePDO
{

    static public function create(string $dsn): PDO;
}

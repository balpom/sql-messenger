<?php

declare(strict_types=1);

namespace Balpom\Messenger\Bridge\Sql\Exception;

use \LogicException;

class PDOException extends LogicException implements SqlExceptionInterface
{

}

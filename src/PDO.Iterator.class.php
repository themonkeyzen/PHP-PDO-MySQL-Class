<?php
class PDOIterator implements Iterator {
    private $position = 0;
    private $pdo;
    private $fetchMode;
    private $nextResult;

    public function __construct(PDOStatement $pdo, $fetchMode = PDO::FETCH_ASSOC) {
        $this->position = 0;
        $this->pdo = $pdo;
        $this->fetchMode = $fetchMode;
    }

    function rewind(): void
    {
        $this->position = 0;
        $this->pdo->execute();
        $this->nextResult = $this->pdo->fetch($this->fetchMode, PDO::FETCH_ORI_NEXT);
    }

    function current(): mixed
    {
        return $this->nextResult;
    }

    function key(): mixed
    {
        return $this->position;
    }

    function next(): void
    {
        ++$this->position;
        $this->nextResult = $this->pdo->fetch($this->fetchMode, PDO::FETCH_ORI_NEXT);
    }

    function valid(): bool
    {
        $invalid = $this->nextResult === false;
        if ($invalid) {
            $this->pdo->closeCursor();
        }
        return !$invalid;
    }
}
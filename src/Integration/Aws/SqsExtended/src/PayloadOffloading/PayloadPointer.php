<?php

namespace AsyncAws\SQSExtended\PayloadOffloading;

interface PayloadPointer
{
    public function asString(): string;
}

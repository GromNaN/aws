<?php

namespace AsyncAws\SQSExtended\PayloadOffloading;

interface PayloadStore
{
    /**
     * Stores payload in a store that has higher payload size limit than that is supported by original payload store.
     *
     * @param string $payload
     * @param string $key
     * @return PayladPointer a pointer that must be used to retrieve the original payload later.
     */
    public function storeOriginalPayload(string $payload, string $key = null): PayloadPointer;

    /**
     * Retrieves the original payload using the given payloadPointer. The pointer must
     * have been obtained using {@link storeOriginalPayload}
     *
     * @param PayloadPointer $pointer
     * @return string original payload
     */
    public function getOriginalPayload(PayloadPointer $pointer): string;

    /**
     * Deletes the original payload using the given payloadPointer. The pointer must
     * have been obtained using {@link storeOriginalPayload}
     *
     * @param PayloadPointer $pointer
     */
    public function deleteOriginalPayload(PayloadPointer $pointer): void;

    /**
     *
     *
     * @param string $pointerString
     * @return PayloadPointer
     */
    public function getPointerFromString(string $pointerString): PayloadPointer;
}

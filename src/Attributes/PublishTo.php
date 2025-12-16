<?php

declare(strict_types=1);

namespace OffloadProject\GooglePubSub\Attributes;

use Attribute;

#[Attribute(Attribute::TARGET_CLASS)]
final class PublishTo
{
    /**
     * Create a new PublishTo attribute.
     *
     * @param  array<string, mixed>  $attributes
     */
    public function __construct(
        public string $topic,
        public array $attributes = []
    ) {}
}

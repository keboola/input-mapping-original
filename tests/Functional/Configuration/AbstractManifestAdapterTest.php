<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional\Configuration;

use Generator;
use PHPUnit\Framework\TestCase;

class AbstractManifestAdapterTest extends TestCase
{
    public function fileOperationsData(): Generator
    {
        yield 'default format' => [
            'format' => null,
            'expectedFilePathname' => __DIR__ . '/data/writeToFile-expected.json',
        ];
        yield 'json format' => [
            'format' => 'json',
            'expectedFilePathname' => __DIR__ . '/data/writeToFile-expected.json',
        ];
        yield 'yaml format' => [
            'format' => 'yaml',
            'expectedFilePathname' => __DIR__ . '/data/writeToFile-expected.yml',
        ];
    }
}

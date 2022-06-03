<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Helper;

use Generator;
use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class ManifestCreatorTest extends TestCase
{
    public function createFileManifestData(): Generator
    {
        yield 'only isPublic' => [
            'isPublic' => true,
            'isSliced' => false,
            'isEncrypted' => false,
        ];
        yield 'only isSliced' => [
            'isPublic' => false,
            'isSliced' => true,
            'isEncrypted' => false,
        ];
        yield 'only isEncrypted' => [
            'isPublic' => false,
            'isSliced' => false,
            'isEncrypted' => true,
        ];
    }

    /**
     * @dataProvider createFileManifestData
     */
    public function testCreateFileManifest(
        bool $expectedIsPublic,
        bool $expectedIsSliced,
        bool $expectedIsEncrypted
    ): void {
        $fileInfo = [
            'id' => 18311387,
            'name' => 'testCreateFileManifest.txt',
            'created' => '2022-05-28T10:31:13+0200',
            'isPublic' => $expectedIsPublic,
            'isSliced' => $expectedIsSliced,
            'isEncrypted' => $expectedIsEncrypted,
            'tags' => ['tag1', 'tag2'],
            'sizeBytes' => 1024,
            'maxAgeDays' => 15,
        ];

        $manifestCreator = new ManifestCreator(self::createMock(Client::class));

        $manifest = $manifestCreator->createFileManifest($fileInfo);

        self::assertSame([
            'id',
            'name',
            'created',
            'is_public',
            'is_encrypted',
            'is_sliced',
            'tags',
            'max_age_days',
            'size_bytes',
        ], array_keys($manifest));

        self::assertSame($fileInfo['id'], $manifest['id']);
        self::assertSame($fileInfo['name'], $manifest['name']);
        self::assertSame($fileInfo['created'], $manifest['created']);
        self::assertSame($fileInfo['isPublic'], $manifest['is_public']);
        self::assertSame($fileInfo['isEncrypted'], $manifest['is_encrypted']);
        self::assertSame($fileInfo['isSliced'], $manifest['is_sliced']);
        self::assertSame($fileInfo['tags'], $manifest['tags']);
        self::assertSame($fileInfo['maxAgeDays'], $manifest['max_age_days']);
        self::assertSame($fileInfo['sizeBytes'], $manifest['size_bytes']);
    }
}

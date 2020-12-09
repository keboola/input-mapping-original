<?php

namespace Reader\Helper;

use Keboola\InputMapping\Reader\Helper\BuildQueryFromConfigurationHelper;
use PHPUnit\Framework\TestCase;

class BuildQueryFromConfigurationHelperTest extends TestCase
{
    public function testGetTagsFromSourceTags()
    {
        self::assertEquals(
            ['componentId: keboola.ex-gmail', 'configurationId: 123'],
            BuildQueryFromConfigurationHelper::getTagsFromSourceTags([
                [
                    'name' => 'componentId: keboola.ex-gmail',
                ],
                [
                    'name' => 'configurationId: 123',
                ],
            ])
        );
    }

    public function testBuildQueryForTags()
    {
        self::assertEquals(
            'tags:"componentId: keboola.ex-gmail" AND tags:"configurationId: 123"',
            BuildQueryFromConfigurationHelper::buildQueryForTags([
                'componentId: keboola.ex-gmail',
                'configurationId: 123',
            ])
        );
    }

    public function testBuildQueryOnlyQuery()
    {
        self::assertEquals(
            'tag:123',
            BuildQueryFromConfigurationHelper::buildQuery([
                'query' => 'tag:123',
            ])
        );
    }

    public function testBuildQueryOnlySourceTags()
    {
        self::assertEquals(
            'tags:"componentId: keboola.ex-gmail" AND tags:"configurationId: 123"',
            BuildQueryFromConfigurationHelper::buildQuery([
                'source' => [
                    'tags' => [
                        [
                            'name' => 'componentId: keboola.ex-gmail',
                        ],
                        [
                            'name' => 'configurationId: 123',
                        ],
                    ],
                ],
            ])
        );
    }

    public function testBuildQuerySourceTagsAndQuery()
    {
        self::assertEquals(
            'tag:123 AND (tags:"componentId: keboola.ex-gmail" AND tags:"configurationId: 123")',
            BuildQueryFromConfigurationHelper::buildQuery([
                'query' => 'tag:123',
                'source' => [
                    'tags' => [
                        [
                            'name' => 'componentId: keboola.ex-gmail',
                        ],
                        [
                            'name' => 'configurationId: 123',
                        ],
                    ],
                ],
            ])
        );
    }
}
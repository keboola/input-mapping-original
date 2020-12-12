<?php

namespace Keboola\InputMapping;

use Keboola\InputMapping\Exception\InvalidInputException;

class NullWorkspaceProvider implements WorkspaceProviderInterface
{
    public function getWorkspaceId($type)
    {
        throw new InvalidInputException(sprintf('Workspace "%s" is not available.', $type));
    }

    public function cleanup()
    {
    }

    public function getCredentials($type)
    {
        return [];
    }
}
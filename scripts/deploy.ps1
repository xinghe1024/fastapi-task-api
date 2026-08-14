param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern(
        '^ghcr\.io/xinghe1024/fastapi-task-api:sha-[0-9a-f]{40}(@sha256:[0-9a-f]{64})?$'
    )]
    [string]$ImageReference
)

$projectDirectory = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectDirectory "compose.prod.yml"
$environmentFile = Join-Path $projectDirectory ".env.production"
$originalApiImage = $env:API_IMAGE
$previousImageReference = $null

function Assert-CommandSucceeded {
    param(
        [int]$ExitCode,
        [string]$FailureMessage
    )

    if ($ExitCode -ne 0) {
        throw $FailureMessage
    }
}

function Wait-ApiReady {
    param(
        [int]$MaximumAttempts = 10,
        [int]$IntervalSeconds = 3
    )

    for ($attemptNumber = 1; $attemptNumber -le $MaximumAttempts; $attemptNumber++) {
        try {
            $null = Invoke-RestMethod `
                -Uri "http://127.0.0.1:8080/health/ready" `
                -TimeoutSec 5

            return $true
        }
        catch {
            if ($attemptNumber -lt $MaximumAttempts) {
                Start-Sleep -Seconds $IntervalSeconds
            }
        }
    }

    return $false
}

try {
    # 进程环境变量优先于 env 文件，用于指定本次部署镜像
    $env:API_IMAGE = $ImageReference

    # 记录当前运行的镜像，失败时方便人工回滚
    $containerIds = @(docker compose `
        -f $composeFile `
        --env-file $environmentFile `
        ps -q api)

    $queryExitCode = $LASTEXITCODE

    Assert-CommandSucceeded `
        -ExitCode $queryExitCode `
        -FailureMessage "无法查询当前 API 容器"

    $existingContainerId = $containerIds |
        Where-Object { $_ } |
        Select-Object -First 1

    if ($existingContainerId) {
        $previousImageReference = docker inspect `
            $existingContainerId `
            --format '{{.Config.Image}}'

        $inspectExitCode = $LASTEXITCODE

        Assert-CommandSucceeded `
            -ExitCode $inspectExitCode `
            -FailureMessage "无法读取当前镜像版本"
    }

    docker compose `
        -f $composeFile `
        --env-file $environmentFile `
        config --quiet

    $configExitCode = $LASTEXITCODE

    Assert-CommandSucceeded `
        -ExitCode $configExitCode `
        -FailureMessage "Compose 配置无效"

    docker compose `
        -f $composeFile `
        --env-file $environmentFile `
        pull api

    $pullExitCode = $LASTEXITCODE

    Assert-CommandSucceeded `
        -ExitCode $pullExitCode `
        -FailureMessage "镜像拉取失败"

    docker compose `
        -f $composeFile `
        --env-file $environmentFile `
        run --rm api `
        alembic upgrade head

    $migrationExitCode = $LASTEXITCODE

    Assert-CommandSucceeded `
        -ExitCode $migrationExitCode `
        -FailureMessage "数据库迁移失败"

    docker compose `
        -f $composeFile `
        --env-file $environmentFile `
        up -d api

    $startupExitCode = $LASTEXITCODE

    Assert-CommandSucceeded `
        -ExitCode $startupExitCode `
        -FailureMessage "API 容器启动失败"

    if (-not (Wait-ApiReady)) {
        throw "API 就绪检查在 30 秒内未通过"
    }

    Write-Host "部署成功：$ImageReference"
    Write-Host "API 就绪检查成功"
}
catch {
    Write-Host "部署失败：$($_.Exception.Message)"

    if ($previousImageReference) {
        Write-Host "部署前镜像：$previousImageReference"
        Write-Host "请确认数据库兼容后再执行应用回滚"
    }

    throw
}
finally {
    # 恢复执行脚本之前的进程环境变量
    if ($null -eq $originalApiImage) {
        Remove-Item Env:API_IMAGE -ErrorAction SilentlyContinue
    }
    else {
        $env:API_IMAGE = $originalApiImage
    }
}

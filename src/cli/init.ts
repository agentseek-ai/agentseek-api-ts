// `agentseek-api init` — scaffold a host project: langgraph.json, an example
// graph, docker-compose.yml and env files, plus dependencies/scripts in the
// host package.json. The Postgres checkpointer needs no wiring here: it is
// swapped in at runtime by a Bun plugin inside the server bootstrap.
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

const PKG_ROOT = resolve(import.meta.dir, '../..')

interface HostPackageJson {
  name?: string
  dependencies?: Record<string, string>
  scripts?: Record<string, string>
  [key: string]: unknown
}

export async function init(cwd: string): Promise<void> {
  const created: string[] = []
  const skipped: string[] = []

  const writeIfAbsent = (relPath: string, content: string): void => {
    const target = resolve(cwd, relPath)
    if (existsSync(target)) {
      skipped.push(relPath)
      return
    }
    writeFileSync(target, content)
    created.push(relPath)
  }

  const copyTemplate = (name: string, relPath: string): void => {
    const target = resolve(cwd, relPath)
    if (existsSync(target)) {
      skipped.push(relPath)
      return
    }
    copyFileSync(resolve(PKG_ROOT, 'templates', name), target)
    created.push(relPath)
  }

  mkdirSync(resolve(cwd, 'graphs'), { recursive: true })
  copyTemplate('langgraph.json', 'langgraph.json')
  copyTemplate('agent.ts', 'graphs/agent.ts')
  copyTemplate('docker-compose.yml', 'docker-compose.yml')
  copyTemplate('env.example', '.env.example')
  writeIfAbsent('.env', readFileSync(resolve(PKG_ROOT, 'templates', 'env.example'), 'utf8'))

  const ourPkg = JSON.parse(readFileSync(resolve(PKG_ROOT, 'package.json'), 'utf8')) as {
    name: string
    version: string
  }

  const hostPkgPath = resolve(cwd, 'package.json')
  const hostPkgExists = existsSync(hostPkgPath)
  const hostPkg: HostPackageJson = hostPkgExists
    ? (JSON.parse(readFileSync(hostPkgPath, 'utf8')) as HostPackageJson)
    : { name: 'my-langgraph-app', version: '0.1.0', type: 'module', private: true }

  hostPkg.dependencies = {
    [ourPkg.name]: `^${ourPkg.version}`,
    '@langchain/langgraph': '^1.4.7',
    '@langchain/core': '^1.2.2',
    ...hostPkg.dependencies,
  }
  hostPkg.scripts = {
    dev: 'agentseek-api dev',
    start: 'agentseek-api serve',
    ...hostPkg.scripts,
  }
  writeFileSync(hostPkgPath, `${JSON.stringify(hostPkg, null, 2)}\n`)
  created.push(hostPkgExists ? 'package.json (updated)' : 'package.json')

  console.log('agentseek-api init complete.\n')
  if (created.length) console.log(`  created: ${created.join(', ')}`)
  if (skipped.length) console.log(`  kept existing: ${skipped.join(', ')}`)
  console.log(`
Next steps:
  1. bun install
  2. docker compose up -d postgres   (or point DATABASE_URL at your own)
  3. bun run dev                     (server on :2024)
`)
}

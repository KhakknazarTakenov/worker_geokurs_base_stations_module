import { Client as SSHClient } from 'ssh2';
import { Client as SCPClient } from 'node-scp';
import Queue from 'bull';
import { logMessage, formatDate, formatTime } from './logger.js';
import path from 'path';
import express from 'express';
import * as dotenv from 'dotenv';
import fs from 'fs/promises';
import './global.js'

const envPath = path.join(process.cwd(), '.env');
dotenv.config({ path: envPath });

// Настраиваем HTTP-сервер для мониторинга
const app = express();
app.get('/health', async (req, res) => {
    try {
        const queueStats = await Promise.all([
            fileOperationsQueue.getWaitingCount(),
            fileOperationsQueue.getActiveCount(),
            fileOperationsQueue.getCompletedCount(),
            fileOperationsQueue.getFailedCount()
        ]).then(([waiting, active, completed, failed]) => ({
            waiting, active, completed, failed
        }));

        res.json({
            status: 'healthy',
            queues: { fileOperations: queueStats }
        });
    } catch (error) {
        await logMessage(globalThis.LOG_TYPES.E, 'worker', `Health check failed: ${error.message}`);
        res.status(500).json({ status: 'error', error: error.message });
    }
});
app.listen(5473, () => console.log('Worker HTTP server running on port 5473'));

// Конфигурация Redis
const redisConfig = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: parseInt(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || ''
};

// Настраиваем очередь для операций с файлами
const fileOperationsQueue = new Queue('fileOperationsQueue', { redis: redisConfig });

// saveTempFile - Сохраняет временный файл в воркере
async function saveTempFile(content, filename) {
    const tmpDir = path.join(process.cwd(), '/tmp');
    await fs.mkdir(tmpDir, { recursive: true });
    const tmpPath = path.join(tmpDir, filename);
    await fs.writeFile(tmpPath, content, 'utf8');
    return tmpPath;
}

// executeSSHCommand - Выполняет SSH-команду с новой сессией
async function executeSSHCommand(command, sftpConfig, useSudo = false) {
    const ssh = new SSHClient();
    let stderr = '';
    let stdout = '';
    await logMessage(globalThis.LOG_TYPES.I, 'ssh', `Executing command: ${command}`);
    return new Promise((resolve, reject) => {
        ssh.on('ready', () => {
            ssh.exec(command, { pty: useSudo }, (err, stream) => {
                if (err) {
                    ssh.end();
                    return reject(err);
                }
                stream.on('data', (data) => {
                    const dataStr = data.toString();
                    if (useSudo && dataStr.includes('password')) {
                        stream.write(`${sftpConfig.sudoPassword}\n`);
                    } else {
                        stdout += dataStr;
                    }
                }).on('close', (code) => {
                    ssh.end();
                    if (code === 0) resolve(stdout.trim());
                    else reject(new Error(`Command "${command}" failed with code ${code}: ${stderr || 'No error message provided'}`));
                }).stderr.on('data', (data) => {
                    stderr += data.toString();
                });
            });
        }).on('error', (err) => {
            ssh.end();
            reject(err);
        }).connect({
            ...sftpConfig,
            keepaliveInterval: 10000,
            keepaliveCountMax: 3
        });
    });
}

// downloadFile - Скачивает файл с удаленного сервера через SCP
async function downloadFile(remotePath, sftpConfig) {
    const tempFileName = `tmp_${path.basename(remotePath)}`;
    const tempRemotePath = `/tmp/${tempFileName}`;
    const localTmpDir = path.join(process.cwd(), '/tmp');
    const localPath = path.join(localTmpDir, tempFileName);

    try {
        // Создаем локальную папку /app/tmp
        await logMessage(globalThis.LOG_TYPES.I, 'downloadFile', `Ensuring local directory ${localTmpDir} exists`);
        await fs.mkdir(localTmpDir, { recursive: true });

        // Создаем временную папку на сервере
        await logMessage(globalThis.LOG_TYPES.I, 'downloadFile', `Ensuring remote directory /tmp exists`);
        await executeSSHCommand(`mkdir -p /tmp`, sftpConfig);

        // Копируем файл во временную папку на сервере с sudo
        await logMessage(globalThis.LOG_TYPES.I, 'downloadFile', `Copying ${remotePath} to ${tempRemotePath}`);
        await executeSSHCommand(`sudo cp ${remotePath} ${tempRemotePath} && sudo chmod 644 ${tempRemotePath}`, sftpConfig, true);
        await logMessage(globalThis.LOG_TYPES.I, 'downloadFile', `Copied ${remotePath} to ${tempRemotePath}`);

        // Скачиваем файл через SCP
        await logMessage(globalThis.LOG_TYPES.I, 'downloadFile', `Downloading ${tempRemotePath} to ${localPath} via SCP`);
        const scp = await SCPClient({
            host: sftpConfig.host,
            port: sftpConfig.port,
            username: sftpConfig.username,
            password: sftpConfig.password
        });
        await scp.downloadFile(tempRemotePath, localPath);
        scp.close();

        // Читаем локальный файл
        const content = await fs.readFile(localPath, 'utf8');
        await logMessage(globalThis.LOG_TYPES.I, 'downloadFile', `Downloaded ${remotePath}`);

        // Удаляем временный файл на сервере с sudo
        await executeSSHCommand(`sudo rm ${tempRemotePath}`, sftpConfig, true);
        await logMessage(globalThis.LOG_TYPES.I, 'downloadFile', `Deleted ${tempRemotePath}`);

        // Удаляем локальный временный файл
        await fs.unlink(localPath);

        return content.split('\n').filter(line => line.trim()).join('\n');
    } catch (error) {
        await logMessage(globalThis.LOG_TYPES.E, 'downloadFile', `Failed to download ${remotePath}: ${error.message}`);
        throw error;
    }
}

// backupAndRotate - Создает бэкап файла и управляет ротацией папок
async function backupAndRotate(sftpConfig, remotePath, filename, taskDate) {
    await logMessage(globalThis.LOG_TYPES.I, 'backup', `Starting backup for ${remotePath}`);
    try {
        const dateFolder = `folder_${formatDate(taskDate)}`;
        const timeFolder = `folder_${formatDate(taskDate)}_${formatTime(taskDate)}`;
        const dateDir = `/home/bitrix/${dateFolder}`;
        const backupDir = `${dateDir}/${timeFolder}`;
        const backupPath = `${backupDir}/${filename}`;
        const backupRoot = '/home/bitrix';
        const tempFileName = `backup_${filename}_${Date.now()}`;
        const tempRemotePath = `/tmp/${tempFileName}`;
        const localPath = path.join(process.cwd(), '/tmp', tempFileName);

        // Проверяем существование /home/bitrix
        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Checking if ${backupRoot} exists`);
        await executeSSHCommand(`ls ${backupRoot}`, sftpConfig);

        // Создаем временную папку на сервере
        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Ensuring remote directory /tmp exists`);
        await executeSSHCommand(`mkdir -p /tmp`, sftpConfig);

        // Создаем дневную папку
        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Creating date directory ${dateDir}`);
        await executeSSHCommand(`mkdir -p ${dateDir}`, sftpConfig);

        // Создаем временную папку
        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Creating backup directory ${backupDir}`);
        await executeSSHCommand(`mkdir -p ${backupDir}`, sftpConfig);

        // Копируем файл во временную папку на сервере с sudo
        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Copying ${remotePath} to ${tempRemotePath}`);
        await executeSSHCommand(`sudo cp ${remotePath} ${tempRemotePath} && sudo chmod 644 ${tempRemotePath}`, sftpConfig, true);

        // Скачиваем файл через SCP
        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Downloading ${tempRemotePath} to ${localPath} via SCP`);
        const scp = await SCPClient({
            host: sftpConfig.host,
            port: sftpConfig.port,
            username: sftpConfig.username,
            password: sftpConfig.password
        });
        await scp.downloadFile(tempRemotePath, localPath);
        scp.close();

        // Загружаем файл в бэкап-папку через SCP
        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Uploading ${localPath} to ${backupPath} via SCP`);
        const scpUpload = await SCPClient({
            host: sftpConfig.host,
            port: sftpConfig.port,
            username: sftpConfig.username,
            password: sftpConfig.password
        });
        await scpUpload.uploadFile(localPath, backupPath);
        scpUpload.close();
        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Backed up ${remotePath} to ${backupPath}`);

        // Удаляем временные файлы с sudo
        await executeSSHCommand(`sudo rm ${tempRemotePath}`, sftpConfig, true);
        await fs.unlink(localPath);

        // Ротация временных папок
        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Starting time folder rotation check in ${dateDir}`);
        let timeFoldersOutput;
        try {
            timeFoldersOutput = await executeSSHCommand(`ls -d ${dateDir}/folder_*_*_*_*`, sftpConfig);
        } catch (error) {
            if (error.message.includes('No such file or directory')) {
                timeFoldersOutput = '';
            } else {
                throw error;
            }
        }
        const timeFolders = timeFoldersOutput
            .split('\n')
            .filter(name => name.match(/folder_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2}$/))
            .map(name => path.basename(name))
            .sort();

        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Found ${timeFolders.length} time folders in ${dateDir}: ${timeFolders.join(', ')}`);

        if (timeFolders.length >= 30) {
            const oldestTimeFolder = timeFolders[0];
            const oldestTimePath = `${dateDir}/${oldestTimeFolder}`;
            await logMessage(globalThis.LOG_TYPES.I, 'backup', `Deleting oldest time folder ${oldestTimePath}`);
            await executeSSHCommand(`rm -rf ${oldestTimePath}`, sftpConfig);
            await logMessage(globalThis.LOG_TYPES.I, 'backup', `Deleted oldest time folder ${oldestTimePath}`);
        } else {
            await logMessage(globalThis.LOG_TYPES.I, 'backup', `No time folder rotation needed, ${timeFolders.length} folders found in ${dateDir}`);
        }

        // Ротация дневных папок
        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Starting date folder rotation check in ${backupRoot}`);
        let dateFoldersOutput;
        try {
            dateFoldersOutput = await executeSSHCommand(`ls -d ${backupRoot}/folder_*`, sftpConfig);
        } catch (error) {
            if (error.message.includes('No such file or directory')) {
                dateFoldersOutput = '';
            } else {
                throw error;
            }
        }
        const dateFolders = dateFoldersOutput
            .split('\n')
            .filter(name => name.match(/folder_\d{4}-\d{2}-\d{2}$/))
            .map(name => path.basename(name))
            .sort();

        await logMessage(globalThis.LOG_TYPES.I, 'backup', `Found ${dateFolders.length} date folders: ${dateFolders.join(', ')}`);

        if (dateFolders.length >= 10) {
            const oldestDateFolder = dateFolders[0];
            const oldestDatePath = `${backupRoot}/${oldestDateFolder}`;
            await logMessage(globalThis.LOG_TYPES.I, 'backup', `Deleting oldest date folder ${oldestDatePath}`);
            await executeSSHCommand(`rm -rf ${oldestDatePath}`, sftpConfig);
            await logMessage(globalThis.LOG_TYPES.I, 'backup', `Deleted oldest date folder ${oldestDatePath}`);
        } else {
            await logMessage(globalThis.LOG_TYPES.I, 'backup', `No date folder rotation needed, ${dateFolders.length} folders found`);
        }
    } catch (error) {
        await logMessage(globalThis.LOG_TYPES.E, 'backup', `Unexpected error in backup for ${remotePath}: ${error.message}`);
    }
    await logMessage(globalThis.LOG_TYPES.I, 'backup', `Finished backup for ${remotePath}`);
}

// processFileOperation - Обрабатывает задачу с файлом (скачивание, модификация, запись)
async function processFileOperation(job) {
    const { operation, params, remotePath, sftpConfig } = job.data;
    const filename = path.basename(remotePath);
    let tempLocalPath;
    const taskDate = params.taskTimestamp ? new Date(params.taskTimestamp) : new Date();
    const tempRemotePath = `/tmp/${filename}`;

    try {
        // Скачиваем текущий файл
        let content = await downloadFile(remotePath, sftpConfig);
        let modifiedContent;

        // Модифицируем содержимое в зависимости от операции
        switch (operation) {
            case 'activate': {
                const { finalLogin, finalPassword, finalGroup, stations } = params;

                if (remotePath.includes('users.aut')) {
                    let usersLines = content.split('\n');
                    const headerUsersLines = [];
                    let foundUsersStart = false;
                    for (const line of usersLines) {
                        headerUsersLines.push(line);
                        if (line.startsWith('admin:')) {
                            foundUsersStart = true;
                            break;
                        }
                    }
                    if (!foundUsersStart) throw new Error('Admin user not found in users.aut');
                    if (usersLines.some(line => line.startsWith(`${finalLogin}:`))) {
                        throw new Error(`Login ${finalLogin} already exists`);
                    }
                    usersLines = [...headerUsersLines, ...usersLines.slice(headerUsersLines.length).filter(line => !line.startsWith(`${finalLogin}:`)), `${finalLogin}:${finalPassword}`];
                    modifiedContent = usersLines.join('\n') + '\n';
                } else if (remotePath.includes('groups.aut')) {
                    let groupsLines = content.split('\n');
                    const headerGroupsLines = [];
                    let foundGroupsStart = false;
                    for (const line of groupsLines) {
                        headerGroupsLines.push(line);
                        if (line.startsWith('gAdmins:')) {
                            foundGroupsStart = true;
                            break;
                        }
                    }
                    if (!foundGroupsStart) throw new Error('gAdmins group not found in groups.aut');
                    groupsLines = [...headerGroupsLines, ...groupsLines.slice(headerGroupsLines.length).filter(line => !line.startsWith(`${finalGroup}:`)), `${finalGroup}:${finalLogin}:1`];
                    modifiedContent = groupsLines.join('\n') + '\n';
                } else if (remotePath.includes('clientmounts.aut')) {
                    const mountsLines = content.split('\n');
                    const headerMountsLines = [];
                    const stationsMap = new Map();
                    let foundStationsStart = false;
                    let currentStation = null;

                    for (const line of mountsLines) {
                        if (!foundStationsStart) {
                            headerMountsLines.push(line);
                            if (line.trim() === '/oper:gAdmins') {
                                foundStationsStart = true;
                            }
                        } else if (line.startsWith('#') && /^#[A-Za-z]/.test(line)) {
                            currentStation = line;
                            stationsMap.set(currentStation, []);
                        } else if (currentStation && line.trim()) {
                            stationsMap.get(currentStation).push(line);
                        }
                    }

                    for (const station of stations) {
                        if (!station.name || !station.formats || !Array.isArray(station.formats)) {
                            await logMessage(globalThis.LOG_TYPES.E, 'activate', `Invalid station data: ${JSON.stringify(station)}`);
                            continue;
                        }
                        const stationCode = `#${station.name.trim()}`;
                        if (!stationsMap.has(stationCode)) {
                            stationsMap.set(stationCode, []);
                            await logMessage(globalThis.LOG_TYPES.I, 'activate', `Added new station: ${stationCode}`);
                        }
                        const formatsList = stationsMap.get(stationCode);
                        for (let format of station.formats) {
                            if (!format || typeof format !== 'string') {
                                await logMessage(globalThis.LOG_TYPES.E, 'activate', `Invalid format in station ${stationCode}: ${format}`);
                                continue;
                            }
                            const mountPoint = (format.startsWith('/') ? format : '/' + format).replace(/:?$/, '');
                            const existing = formatsList.find(l => l.startsWith(mountPoint + ':'));
                            if (existing) {
                                const [fmt, groups] = existing.split(':');
                                const groupList = groups ? groups.split(',').filter(g => g) : [];
                                if (!groupList.includes(finalGroup)) {
                                    groupList.push(finalGroup);
                                    const updatedLine = `${fmt}:${groupList.join(',')}`;
                                    formatsList[formatsList.indexOf(existing)] = updatedLine;
                                }
                            } else {
                                const newLine = `${mountPoint}:${finalGroup}`;
                                formatsList.push(newLine);
                                await logMessage(globalThis.LOG_TYPES.I, 'activate', `Added new format ${newLine}`);
                            }
                        }
                        stationsMap.set(stationCode, formatsList);
                    }

                    let newMounts = [...headerMountsLines];
                    for (const [station, formats] of stationsMap) {
                        newMounts.push(station);
                        if (formats.length > 0) {
                            newMounts.push(...formats);
                        }
                        newMounts.push('');
                    }
                    modifiedContent = newMounts.join('\n').trim() + '\n';
                    if (!modifiedContent.trim() || modifiedContent.split('\n').length <= headerMountsLines.length) {
                        throw new Error('Failed to generate valid clientmounts.aut content');
                    }
                }
                break;
            }
            case 'deactivate': {
                const { login, group } = params;

                if (remotePath.includes('users.aut')) {
                    let usersLines = content.split('\n');
                    const headerUsersLines = [];
                    let foundUsersStart = false;
                    for (const line of usersLines) {
                        headerUsersLines.push(line);
                        if (line.startsWith('admin:')) {
                            foundUsersStart = true;
                            break;
                        }
                    }
                    if (!foundUsersStart) throw new Error('Admin user not found in users.aut');
                    usersLines = [...headerUsersLines, ...usersLines.slice(headerUsersLines.length).filter(line => !line.startsWith(`${login}:`))];
                    modifiedContent = usersLines.join('\n') + '\n';
                } else if (remotePath.includes('groups.aut')) {
                    let groupsLines = content.split('\n');
                    const headerGroupsLines = [];
                    let foundGroupsStart = false;
                    for (const line of groupsLines) {
                        headerGroupsLines.push(line);
                        if (line.startsWith('gAdmins:')) {
                            foundGroupsStart = true;
                            break;
                        }
                    }
                    if (!foundGroupsStart) throw new Error('gAdmins group not found in groups.aut');
                    groupsLines = [...headerGroupsLines, ...groupsLines.slice(headerGroupsLines.length).filter(line => !line.startsWith(`${group}:`))];
                    modifiedContent = groupsLines.join('\n') + '\n';
                } else if (remotePath.includes('clientmounts.aut')) {
                    const mountsLines = content.split('\n');
                    const headerMountsLines = [];
                    const stationsMap = new Map();
                    let foundStationsStart = false;
                    let currentStation = null;

                    for (const line of mountsLines) {
                        if (!foundStationsStart) {
                            headerMountsLines.push(line);
                            if (line.trim() === '/oper:gAdmins') {
                                foundStationsStart = true;
                            }
                        } else if (line.startsWith('#') && /^#[A-Za-z]/.test(line)) {
                            currentStation = line;
                            stationsMap.set(currentStation, []);
                        } else if (currentStation && line.trim()) {
                            stationsMap.get(currentStation).push(line);
                        }
                    }

                    for (const [station, formats] of stationsMap.entries()) {
                        const updatedFormats = formats.map(formatLine => {
                            const [fmt, groupsStr] = formatLine.includes(':') ? formatLine.split(':') : [formatLine, ''];
                            const groupsArr = groupsStr.split(',').filter(g => g !== group && g);
                            return groupsArr.length > 0 ? `${fmt}:${groupsArr.join(',')}` : null;
                        }).filter(Boolean);
                        stationsMap.set(station, updatedFormats);
                    }

                    for (const [station, formats] of [...stationsMap.entries()]) {
                        if (formats.length === 0) {
                            stationsMap.delete(station);
                            await logMessage(globalThis.LOG_TYPES.I, 'deactivate', `Deleted station ${station} with no remaining formats`);
                        }
                    }

                    let newMounts = [...headerMountsLines];
                    for (const [station, formats] of stationsMap) {
                        newMounts.push(station);
                        newMounts.push(...formats);
                        newMounts.push('');
                    }
                    modifiedContent = newMounts.join('\n').trim() + '\n';
                    if (!modifiedContent.trim() || modifiedContent.split('\n').length <= headerMountsLines.length) {
                        throw new Error('Failed to generate valid clientmounts.aut content');
                    }
                }
                break;
            }
            case 'handle_new_station_creation': {
                const { clientmountField, formats, groups } = params;

                if (remotePath.includes('clientmounts.aut')) {
                    const mountsLines = content.split('\n');
                    const headerMountsLines = [];
                    const stationsMap = new Map();
                    let foundStationsStart = false;
                    let currentStation = null;

                    for (const line of mountsLines) {
                        if (!foundStationsStart) {
                            headerMountsLines.push(line);
                            if (line.trim() === '/oper:gAdmins') {
                                foundStationsStart = true;
                            }
                        } else if (line.startsWith('#') && /^#[A-Za-z]/.test(line)) {
                            currentStation = line;
                            stationsMap.set(currentStation, []);
                        } else if (currentStation && line.trim()) {
                            stationsMap.get(currentStation).push(line);
                        }
                    }

                    const codeLine = `${clientmountField}`;
                    if (!stationsMap.has(codeLine)) {
                        stationsMap.set(codeLine, []);
                    }
                    const formatsList = stationsMap.get(codeLine);
                    for (let format of formats) {
                        const existing = formatsList.find(l => l.startsWith(format + ':'));
                        if (existing) {
                            const [fmt, groupsStr] = existing.split(':');
                            const existingGroups = groupsStr.split(',').filter(Boolean);
                            const mergedGroups = [...new Set([...existingGroups, ...groups.split(',')])];
                            const idx = formatsList.indexOf(existing);
                            formatsList[idx] = `${fmt}:${mergedGroups.join(',')}`;
                        } else {
                            formatsList.push(`${format}:${groups}`);
                        }
                    }
                    stationsMap.set(codeLine, formatsList);

                    let newMounts = [...headerMountsLines];
                    for (const [station, formats] of stationsMap) {
                        newMounts.push(station);
                        if (formats.length > 0) {
                            newMounts.push(...formats);
                        }
                        newMounts.push('');
                    }
                    modifiedContent = newMounts.join('\n').trim() + '\n';
                    if (!modifiedContent.trim() || modifiedContent.split('\n').length <= headerMountsLines.length) {
                        throw new Error('Failed to generate valid clientmounts.aut content');
                    }
                }
                break;
            }
            case 'webhook_handle_station_edit': {
                const { stations, group, activeStationCodes } = params;

                if (remotePath.includes('clientmounts.aut')) {
                    const mountsLines = content.split('\n');
                    const headerMountsLines = [];
                    const stationsMap = new Map();
                    let foundStationsStart = false;
                    let currentStation = null;

                    for (const line of mountsLines) {
                        if (!foundStationsStart) {
                            headerMountsLines.push(line);
                            if (line.trim() === '/oper:gAdmins') {
                                foundStationsStart = true;
                            }
                        } else if (line.startsWith('#') && /^#[A-Za-z]/.test(line)) {
                            currentStation = line;
                            stationsMap.set(currentStation, []);
                        } else if (currentStation && line.trim()) {
                            stationsMap.get(currentStation).push(line);
                        }
                    }

                    for (const station of stations) {
                        const clientmountField = station.ufCrm6_1747732721580;
                        if (!clientmountField || !clientmountField.startsWith('#')) {
                            await logMessage(globalThis.LOG_TYPES.E, 'webhook_handle_station_edit', `Invalid or missing clientmountField for station ${station.id}, skipping`);
                            continue;
                        }

                        const formats = station.formats;
                        if (formats.length === 0) {
                            await logMessage(globalThis.LOG_TYPES.E, 'webhook_handle_station_edit', `No formats provided for station ${station.id}, skipping`);
                            continue;
                        }

                        if (!stationsMap.has(clientmountField)) {
                            stationsMap.set(clientmountField, []);
                            await logMessage(globalThis.LOG_TYPES.I, 'webhook_handle_station_edit', `Added new station: ${clientmountField}`);
                        }

                        const formatsList = stationsMap.get(clientmountField);
                        for (const format of formats) {
                            const mountPoint = format.replace(/:?$/, '');
                            const existing = formatsList.find(l => l.startsWith(mountPoint + ':'));
                            if (existing) {
                                const [fmt, groupsStr = ''] = existing.includes(':') ? existing.split(':') : [existing, ''];
                                const existingGroups = groupsStr.split(',').filter(Boolean);
                                if (!existingGroups.includes(group)) {
                                    const merged = [...new Set([...existingGroups, group])];
                                    formatsList[formatsList.indexOf(existing)] = `${fmt}:${merged.join(',')}`;
                                }
                            } else {
                                const newLine = `${mountPoint}:${group}`;
                                formatsList.push(newLine);
                                await logMessage(globalThis.LOG_TYPES.I, 'webhook_handle_station_edit', `Added new format ${newLine}`);
                            }
                        }
                        stationsMap.set(clientmountField, formatsList);
                    }

                    for (const [station, formats] of [...stationsMap.entries()]) {
                        if (!activeStationCodes.includes(station)) {
                            const updatedFormats = formats.map(formatLine => {
                                const [fmt, groupsStr = ''] = formatLine.includes(':') ? formatLine.split(':') : [formatLine, ''];
                                const groupsArr = groupsStr.split(',').filter(g => g !== group && g);
                                return groupsArr.length > 0 ? `${fmt}:${groupsArr.join(',')}` : null;
                            }).filter(Boolean);
                            if (updatedFormats.length > 0) {
                                stationsMap.set(station, updatedFormats);
                                await logMessage(globalThis.LOG_TYPES.I, 'webhook_handle_station_edit', `Removed group ${group} from non-active station ${station}`);
                            } else {
                                stationsMap.delete(station);
                                await logMessage(globalThis.LOG_TYPES.I, 'webhook_handle_station_edit', `Deleted non-active station ${station} with no remaining groups`);
                            }
                        }
                    }

                    let newMounts = [...headerMountsLines];
                    for (const [station, formats] of stationsMap) {
                        newMounts.push(station);
                        if (formats.length > 0) {
                            newMounts.push(...formats);
                        }
                        newMounts.push('');
                    }
                    modifiedContent = newMounts.join('\n').trim() + '\n';
                    if (!modifiedContent.trim() || modifiedContent.split('\n').length <= headerMountsLines.length) {
                        throw new Error('Failed to generate valid clientmounts.aut content');
                    }
                }
                break;
            }
            default:
                throw new Error(`Unsupported operation: ${operation}`);
        }

        // Создаем временный файл
        tempLocalPath = await saveTempFile(modifiedContent, filename);

        // Создаем временную папку на сервере
        await logMessage(globalThis.LOG_TYPES.I, 'worker', `Ensuring remote directory /tmp exists for job ${job.id}`);
        await executeSSHCommand(`mkdir -p /tmp`, sftpConfig);

        // Проверяем права на удаленную папку
        await logMessage(globalThis.LOG_TYPES.I, 'worker', `Checking permissions for /tmp`);
        await executeSSHCommand(`ls -ld /tmp`, sftpConfig);

        // Загружаем файл через SCP
        await logMessage(globalThis.LOG_TYPES.I, 'worker', `Uploading ${tempLocalPath} to ${tempRemotePath} via SCP, job ID: ${job.id}`);
        const scp = await SCPClient({
            host: sftpConfig.host,
            port: sftpConfig.port,
            username: sftpConfig.username,
            password: sftpConfig.password
        });
        await scp.uploadFile(tempLocalPath, tempRemotePath);
        scp.close();
        await logMessage(globalThis.LOG_TYPES.I, 'worker', `Uploaded to ${tempRemotePath}, job ID: ${job.id}`);

        // Проверяем целостность
        const remoteSize = parseInt(await executeSSHCommand(`ls -l ${tempRemotePath} | awk '{print $5}'`, sftpConfig));
        const expectedSize = Buffer.from(modifiedContent).length;
        if (remoteSize !== expectedSize) {
            throw new Error(`Incomplete file upload: ${tempRemotePath} size ${remoteSize}, expected ${expectedSize}`);
        }
        await logMessage(globalThis.LOG_TYPES.I, 'worker', `Verified file size in /tmp: ${remoteSize} bytes`);

        // Создаем бэкап и управляем ротацией
        await backupAndRotate(sftpConfig, remotePath, filename, taskDate);

        // Выполняем копирование с sudo
        await logMessage(globalThis.LOG_TYPES.I, 'worker', `Copying to ${remotePath}`);
        await executeSSHCommand(`sudo cp ${tempRemotePath} ${remotePath}`, sftpConfig, true);
        await logMessage(globalThis.LOG_TYPES.I, 'worker', `Copied to ${remotePath}`);

        // Проверяем содержимое записанного файла
        await logMessage(globalThis.LOG_TYPES.I, 'worker', `Verifying copied file size at ${remotePath}`);
        const finalSize = parseInt(await executeSSHCommand(`ls -l ${remotePath} | awk '{print $5}'`, sftpConfig));
        if (finalSize !== expectedSize) {
            throw new Error(`Incomplete file copy: ${remotePath} size ${finalSize}, expected ${expectedSize}`);
        }
        await logMessage(globalThis.LOG_TYPES.I, 'worker', `Verified copied file size: ${finalSize} bytes`);

        // Удаляем временный файл на сервере с sudo
        await executeSSHCommand(`sudo rm ${tempRemotePath}`, sftpConfig, true);
        await logMessage(globalThis.LOG_TYPES.I, 'worker', `Deleted ${tempRemotePath}`);

        return { success: true };
    } catch (error) {
        await logMessage(globalThis.LOG_TYPES.E, 'worker', `Failed to process job ${job.id} for ${remotePath}: ${error.message}`);
        throw error;
    } finally {
        if (tempLocalPath) {
            await fs.unlink(tempLocalPath).catch(e =>
                logMessage(globalThis.LOG_TYPES.E, 'worker', `Failed to delete local file ${tempLocalPath}: ${e.message}`)
            );
        }
    }
}

// Регистрируем обработчик для очереди
fileOperationsQueue.process(1, async (job) => await processFileOperation(job));

// Логирование событий очереди
fileOperationsQueue.on('failed', async (job, err) => {
    await logMessage(globalThis.LOG_TYPES.E, 'worker', `Job ${job.id} in fileOperationsQueue failed: ${err.message}`);
});
fileOperationsQueue.on('completed', async (job) => {
    await logMessage(globalThis.LOG_TYPES.I, 'worker', `Job ${job.id} in fileOperationsQueue completed`);
});

// Закрытие соединений с Redis
process.on('SIGTERM', async () => {
    await fileOperationsQueue.close();
    process.exit(0);
});

console.log('Worker started, listening for queue tasks');
// worker.js
import Client from 'ssh2-sftp-client';
import { Client as SSHClient } from 'ssh2';
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
        await logMessage(LOG_TYPES.E, 'worker', `Health check failed: ${error.message}`);
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

// Настраиваем единую очередь для операций с файлами
const fileOperationsQueue = new Queue('fileOperationsQueue', { redis: redisConfig });

// saveTempFile - Сохраняет временный файл в воркере
async function saveTempFile(content, filename) {
    const tmpDir = path.join(process.cwd(), '/tmp');
    await fs.mkdir(tmpDir, { recursive: true });
    const tmpPath = path.join(tmpDir, filename);
    await fs.writeFile(tmpPath, content, 'utf8');
    return tmpPath;
}

// downloadFile - Скачивает файл с удаленного сервера
async function downloadFile(remotePath, sftpConfig) {
    const sftp = new Client();
    try {
        await sftp.connect({
            ...sftpConfig,
            retries: 3,
            readyTimeout: 10000,
            keepaliveInterval: 10000,
            keepaliveCountMax: 3
        });
        const content = await sftp.get(remotePath);
        return content.toString().split('\n').filter(line => line.trim()).join('\n');
    } catch (error) {
        await logMessage(LOG_TYPES.E, 'downloadFile', `Failed to download ${remotePath}: ${error.message}`);
        throw error;
    } finally {
        await sftp.end();
    }
}

// backupAndRotate - Создает бэкап файла и управляет ротацией папок
async function backupAndRotate(sftp, ssh, sftpConfig, remotePath, filename, taskDate) {
    await logMessage(LOG_TYPES.I, 'backup', `Starting backup for ${remotePath}`);
    try {
        const dateFolder = `folder_${formatDate(taskDate)}`;
        const timeFolder = `folder_${formatDate(taskDate)}_${formatTime(taskDate)}`;
        const dateDir = `/home/casteradmin/${dateFolder}`;
        const backupDir = `${dateDir}/${timeFolder}`;
        const backupPath = `${backupDir}/${filename}`;
        const backupRoot = '/home/casteradmin';

        // Проверяем доступность /home/casteradmin
        await logMessage(LOG_TYPES.I, 'backup', `Checking access to ${backupRoot}`);
        try {
            await sftp.stat(backupRoot);
        } catch (error) {
            await logMessage(LOG_TYPES.E, 'backup', `Cannot access ${backupRoot}: ${error.message}`);
            return;
        }

        // Создаем дневную папку
        await logMessage(LOG_TYPES.I, 'backup', `Creating date directory ${dateDir}`);
        try {
            await sftp.mkdir(dateDir, true);
            await logMessage(LOG_TYPES.I, 'backup', `Created date directory ${dateDir}`);
        } catch (error) {
            if (error.code !== 4) { // Код 4 - папка уже существует
                await logMessage(LOG_TYPES.E, 'backup', `Failed to create date directory ${dateDir}: ${error.message}`);
                return;
            }
            await logMessage(LOG_TYPES.I, 'backup', `Date directory ${dateDir} already exists`);
        }

        // Создаем временную папку
        await logMessage(LOG_TYPES.I, 'backup', `Creating backup directory ${backupDir}`);
        try {
            await sftp.mkdir(backupDir, true);
            await logMessage(LOG_TYPES.I, 'backup', `Created backup directory ${backupDir}`);
        } catch (error) {
            await logMessage(LOG_TYPES.E, 'backup', `Failed to create backup directory ${backupDir}: ${error.message}`);
            return;
        }

        // Копируем текущий файл в папку бэкапа
        await logMessage(LOG_TYPES.I, 'backup', `Copying ${remotePath} to ${backupPath}`);
        try {
            await new Promise((resolve, reject) => {
                let stderr = '';
                ssh.exec(`sudo cp ${remotePath} ${backupPath}`, { pty: true }, (err, stream) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    stream.on('close', (code) => {
                        if (code === 0) resolve();
                        else reject(new Error(`sudo cp to backup failed with code ${code}: ${stderr}`));
                    }).on('data', (data) => {
                        const dataStr = data.toString();
                        if (dataStr.includes('password')) {
                            stream.write(`${sftpConfig.sudoPassword}\n`);
                        }
                    }).stderr.on('data', (data) => {
                        stderr += data;
                    });
                });
            });
            await logMessage(LOG_TYPES.I, 'backup', `Backed up ${remotePath} to ${backupPath}`);
        } catch (error) {
            await logMessage(LOG_TYPES.E, 'backup', `Failed to copy ${remotePath} to ${backupPath}: ${error.message}`);
            return;
        }

        // Ротация временных папок в дневной папке
        await logMessage(LOG_TYPES.I, 'backup', `Starting time folder rotation check in ${dateDir}`);
        let timeDirList;
        try {
            timeDirList = await sftp.list(dateDir);
            await logMessage(LOG_TYPES.I, 'backup', `Listed time directories in ${dateDir}`);
        } catch (error) {
            await logMessage(LOG_TYPES.E, 'backup', `Failed to list time directories in ${dateDir}: ${error.message}`);
            return;
        }

        const timeFolders = timeDirList
            .filter(item => item.type === 'd' && item.name.match(/^folder_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2}$/))
            .sort((a, b) => a.name.localeCompare(b.name)); // Сортировка по имени (дата и время)

        await logMessage(LOG_TYPES.I, 'backup', `Found ${timeFolders.length} time folders in ${dateDir}: ${timeFolders.map(f => f.name).join(', ')}`);

        if (timeFolders.length >= 30) {
            const oldestTimeFolder = timeFolders[0].name;
            const oldestTimePath = `${dateDir}/${oldestTimeFolder}`;
            await logMessage(LOG_TYPES.I, 'backup', `Deleting oldest time folder ${oldestTimePath}`);
            try {
                await sftp.rmdir(oldestTimePath, true);
                await logMessage(LOG_TYPES.I, 'backup', `Deleted oldest time folder ${oldestTimePath}`);
            } catch (error) {
                await logMessage(LOG_TYPES.E, 'backup', `Failed to delete ${oldestTimePath}: ${error.message}`);
            }
        } else {
            await logMessage(LOG_TYPES.I, 'backup', `No time folder rotation needed, ${timeFolders.length} folders found in ${dateDir}`);
        }

        // Ротация дневных папок
        await logMessage(LOG_TYPES.I, 'backup', `Starting date folder rotation check in ${backupRoot}`);
        let dateDirList;
        try {
            dateDirList = await sftp.list(backupRoot);
            await logMessage(LOG_TYPES.I, 'backup', `Listed date directories in ${backupRoot}`);
        } catch (error) {
            await logMessage(LOG_TYPES.E, 'backup', `Failed to list date directories in ${backupRoot}: ${error.message}`);
            return;
        }

        const dateFolders = dateDirList
            .filter(item => item.type === 'd' && item.name.match(/^folder_\d{4}-\d{2}-\d{2}$/))
            .sort((a, b) => a.name.localeCompare(b.name)); // Сортировка по имени (дата)

        await logMessage(LOG_TYPES.I, 'backup', `Found ${dateFolders.length} date folders: ${dateFolders.map(f => f.name).join(', ')}`);

        if (dateFolders.length >= 10) {
            const oldestDateFolder = dateFolders[0].name;
            const oldestDatePath = `${backupRoot}/${oldestDateFolder}`;
            await logMessage(LOG_TYPES.I, 'backup', `Deleting oldest date folder ${oldestDatePath}`);
            try {
                await sftp.rmdir(oldestDatePath, true);
                await logMessage(LOG_TYPES.I, 'backup', `Deleted oldest date folder ${oldestDatePath}`);
            } catch (error) {
                await logMessage(LOG_TYPES.E, 'backup', `Failed to delete ${oldestDatePath}: ${error.message}`);
            }
        } else {
            await logMessage(LOG_TYPES.I, 'backup', `No date folder rotation needed, ${dateFolders.length} folders found`);
        }
    } catch (error) {
        await logMessage(LOG_TYPES.E, 'backup', `Unexpected error in backup for ${remotePath}: ${error.message}`);
    }
    await logMessage(LOG_TYPES.I, 'backup', `Finished backup for ${remotePath}`);
}

// processFileOperation - Обрабатывает задачу с файлом (скачивание, модификация, запись)
async function processFileOperation(job) {
    const { operation, params, remotePath, sftpConfig } = job.data;
    const filename = path.basename(remotePath);
    let tempLocalPath;
    const taskDate = new Date(); // Фиксируем время начала задачи

    try {
        // Скачиваем текущий файл
        let content = await downloadFile(remotePath, sftpConfig);
        let modifiedContent;

        // Модифицируем содержимое в зависимости от операции
        switch (operation) {
            case 'activate': {
                const { finalLogin, finalPassword, finalGroup, stations } = params;

                // Модификация users.aut
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
                }

                // Модификация groups.aut
                else if (remotePath.includes('groups.aut')) {
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
                    groupsLines = [...headerUsersLines, ...groupsLines.slice(headerGroupsLines.length).filter(line => !line.startsWith(`${finalGroup}:`)), `${finalGroup}:${finalLogin}:1`];
                    modifiedContent = groupsLines.join('\n') + '\n';
                }

                // Модификация clientmounts.aut
                else if (remotePath.includes('clientmounts.aut')) {
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
                            await logMessage(LOG_TYPES.E, 'activate', `Invalid station data: ${JSON.stringify(station)}`);
                            continue;
                        }
                        const stationCode = `#${station.name.trim()}`;
                        if (!stationsMap.has(stationCode)) {
                            stationsMap.set(stationCode, []);
                            await logMessage(LOG_TYPES.I, 'activate', `Added new station: ${stationCode}`);
                        }
                        const formatsList = stationsMap.get(stationCode);
                        for (let format of station.formats) {
                            if (!format || typeof format !== 'string') {
                                await logMessage(LOG_TYPES.E, 'activate', `Invalid format in station ${stationCode}: ${format}`);
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
                                await logMessage(LOG_TYPES.I, 'activate', `Added new format ${newLine}`);
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

                // Модификация users.aut
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
                }

                // Модификация groups.aut
                else if (remotePath.includes('groups.aut')) {
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
                }

                // Модификация clientmounts.aut
                else if (remotePath.includes('clientmounts.aut')) {
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
                            await logMessage(LOG_TYPES.I, 'deactivate', `Deleted station ${station} with no remaining formats`);
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

                // Модификация clientmounts.aut
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

                // Модификация clientmounts.aut
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
                            await logMessage(LOG_TYPES.E, 'webhook_handle_station_edit', `Invalid or missing clientmountField for station ${station.id}, skipping`);
                            continue;
                        }

                        const formats = station.formats;
                        if (formats.length === 0) {
                            await logMessage(LOG_TYPES.E, 'webhook_handle_station_edit', `No formats provided for station ${station.id}, skipping`);
                            continue;
                        }

                        if (!stationsMap.has(clientmountField)) {
                            stationsMap.set(clientmountField, []);
                            await logMessage(LOG_TYPES.I, 'webhook_handle_station_edit', `Added new station: ${clientmountField}`);
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
                                await logMessage(LOG_TYPES.I, 'webhook_handle_station_edit', `Added new format ${newLine}`);
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
                                await logMessage(LOG_TYPES.I, 'webhook_handle_station_edit', `Removed group ${group} from non-active station ${station}`);
                            } else {
                                stationsMap.delete(station);
                                await logMessage(LOG_TYPES.I, 'webhook_handle_station_edit', `Deleted non-active station ${station} with no remaining groups`);
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
        const tempRemotePath = `/home/casteradmin/${filename}`;
        const sftp = new Client();
        const ssh = new SSHClient();

        try {
            // Подключаемся к SFTP
            await sftp.connect({
                ...sftpConfig,
                retries: 3,
                readyTimeout: 10000,
                keepaliveInterval: 10000,
                keepaliveCountMax: 3
            });

            // Загружаем файл в /tmp на удаленном сервере
            await logMessage(LOG_TYPES.I, 'worker', `Uploading to ${tempRemotePath}, job ID: ${job.id}`);
            await sftp.put(tempLocalPath, tempRemotePath);
            await logMessage(LOG_TYPES.I, 'worker', `Uploaded to ${tempRemotePath}, job ID: ${job.id}`);

            // Проверяем целостность
            const stats = await sftp.stat(tempRemotePath);
            if (stats.size !== Buffer.from(modifiedContent).length) {
                throw new Error(`Incomplete file upload: ${tempRemotePath} size ${stats.size}, expected ${Buffer.from(modifiedContent).length}`);
            }
            await logMessage(LOG_TYPES.I, 'worker', `Verified file size in /tmp: ${stats.size} bytes`);

            // Подключаемся к SSH
            await new Promise((resolve, reject) => {
                ssh.on('ready', resolve)
                    .on('error', reject)
                    .connect({
                        ...sftpConfig,
                        keepaliveInterval: 10000,
                        keepaliveCountMax: 3
                    });
            });

            // Создаем бэкап и управляем ротацией
            await backupAndRotate(sftp, ssh, sftpConfig, remotePath, filename, taskDate);

            // Выполняем sudo cp
            await logMessage(LOG_TYPES.I, 'worker', `Copying to ${remotePath}`);
            const command = `sudo cp ${tempRemotePath} ${remotePath}`;
            let stderr = '';
            await new Promise((resolve, reject) => {
                ssh.exec(command, { pty: true }, (err, stream) => {
                    if (err) reject(err);
                    stream.on('close', (code) => {
                        if (code === 0) resolve();
                        else reject(new Error(`sudo cp failed: ${stderr}`));
                    }).on('data', (data) => {
                        if (data.toString().includes('password')) {
                            stream.write(`${sftpConfig.sudoPassword}\n`);
                        }
                    }).stderr.on('data', (data) => stderr += data);
                });
            });
            await logMessage(LOG_TYPES.I, 'worker', `Copied to ${remotePath}`);

            // Проверяем содержимое записанного файла
            await logMessage(LOG_TYPES.I, 'worker', `Verifying copied file size at ${remotePath}`);
            const remoteStats = await sftp.stat(remotePath);
            if (remoteStats.size !== Buffer.from(modifiedContent).length) {
                throw new Error(`Incomplete file copy: ${remotePath} size ${remoteStats.size}, expected ${Buffer.from(modifiedContent).length}`);
            }
            await logMessage(LOG_TYPES.I, 'worker', `Verified copied file size: ${remoteStats.size} bytes`);

            // Удаляем временный файл на удаленном сервере
            await sftp.delete(tempRemotePath);
            await logMessage(LOG_TYPES.I, 'worker', `Deleted ${tempRemotePath}`);

            return { success: true };
        } finally {
            try { await sftp.end(); } catch (e) {
                await logMessage(LOG_TYPES.E, 'worker', `Failed to close SFTP: ${e.message}`);
            }
            try { ssh.end(); } catch (e) {
                await logMessage(LOG_TYPES.E, 'worker', `Failed to close SSH: ${e.message}`);
            }
        }
    } catch (error) {
        await logMessage(LOG_TYPES.E, 'worker', `Failed to process job ${job.id} for ${remotePath}: ${error.message}`);
        throw error;
    } finally {
        // Удаляем локальный временный файл
        if (tempLocalPath) {
            await fs.unlink(tempLocalPath).catch(e =>
                logMessage(LOG_TYPES.E, 'worker', `Failed to delete local file ${tempLocalPath}: ${e.message}`)
            );
        }
    }
}

// Регистрируем обработчик для очереди с concurrency: 1
fileOperationsQueue.process(1, async (job) => await processFileOperation(job));

// Логирование событий очереди
fileOperationsQueue.on('failed', async (job, err) => {
    await logMessage(LOG_TYPES.E, 'worker', `Job ${job.id} in fileOperationsQueue failed: ${err.message}`);
});
fileOperationsQueue.on('completed', async (job) => {
    await logMessage(LOG_TYPES.I, 'worker', `Job ${job.id} in fileOperationsQueue completed`);
});

// Закрытие соединений с Redis при завершении
process.on('SIGTERM', async () => {
    await fileOperationsQueue.close();
    process.exit(0);
});

console.log('Worker started, listening for queue tasks');
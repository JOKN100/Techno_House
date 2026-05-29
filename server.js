require('dotenv').config();

const express   = require('express');
const http      = require('http');
const { Server }= require('socket.io');
const mongoose  = require('mongoose');
const jwt       = require('jsonwebtoken');
const cors      = require('cors');
const bcrypt    = require('bcrypt');
const rateLimit = require('express-rate-limit');
const helmet    = require('helmet');
const mqtt      = require('mqtt');

// ─────────────────────────────────────────────
//  App & Server
// ─────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
    cors: {
        origin: process.env.ALLOWED_ORIGINS
            ? process.env.ALLOWED_ORIGINS.split(',')
            : '*',
        methods: ['GET', 'POST']
    }
});

// ─────────────────────────────────────────────
//  Global Middleware
// ─────────────────────────────────────────────
app.use(helmet());                        // ✅ Security headers
app.use(express.json({ limit: '10kb' })); // ✅ حد لحجم الـ body
app.use(cors({
    origin: process.env.ALLOWED_ORIGINS
        ? process.env.ALLOWED_ORIGINS.split(',')
        : '*'
}));

const PORT = process.env.PORT || 3000;
let   mqttClient;

// ─────────────────────────────────────────────
//  1. Models
// ─────────────────────────────────────────────
const userSchema = new mongoose.Schema({
    fullName:  { type: String, required: true, trim: true },
    email:     { type: String, required: true, unique: true, lowercase: true, trim: true },
    password:  { type: String, required: true },
    houseCode: { type: String, required: true },
    settings:  {
        notifications: { type: Boolean, default: true },
        darkMode:      { type: Boolean, default: false }
    }
});

const logSchema = new mongoose.Schema({
    sensorName:  { type: String, required: true },
    value:       { type: Number },
    roomKey:     String,
    houseCode:   { type: String, required: true },
    eventType:   { type: String, enum: ['sensor', 'control', 'door'], default: 'sensor' },
    triggeredBy: { type: String, default: 'system' },
    timestamp:   { type: Date,   default: Date.now, index: true }
});

const roomSchema = new mongoose.Schema({
    name:      { type: String, required: true },
    key:       { type: String, required: true },
    houseCode: { type: String, required: true }
});

const deviceSchema = new mongoose.Schema({
    name:      { type: String, required: true },
    type:      { type: String, required: true },
    roomKey:   { type: String, required: true },
    houseCode: { type: String, required: true },
    status:    { type: Boolean, default: false },
    value:     { type: Number,  default: 0 },
    // ✅ FIX: pinCode مش required للمبات والمراوح — فقط للأبواب
    pinCode:   { type: String, default: '' }
});

const scheduleSchema = new mongoose.Schema({
    houseCode: { type: String, required: true },
    deviceId:  { type: mongoose.Schema.Types.ObjectId, ref: 'Device', required: true },
    action:    { status: Boolean, value: Number },
    cronTime:  { type: String, required: true },
    days:      [{ type: String }],
    isActive:  { type: Boolean, default: true },
    label:     { type: String }
});

const User     = mongoose.model('User',     userSchema);
const Log      = mongoose.model('Log',      logSchema);
const Room     = mongoose.model('Room',     roomSchema);
const Device   = mongoose.model('Device',   deviceSchema);
const Schedule = mongoose.model('Schedule', scheduleSchema);

// ─────────────────────────────────────────────
//  2. MQTT Publish Helper
// ─────────────────────────────────────────────
function mqttPublish(topic, payload) {
    if (!mqttClient) {
        console.warn(`⚠️  MQTT client not initialized — skipped: ${topic}`);
        return;
    }
    // ✅ FIX: لا نوقف على connected check — الـ mqtt package بيـ queue الرسائل
    mqttClient.publish(topic, String(payload), { qos: 1, retain: false }, (err) => {
        if (err) console.error(`❌ MQTT Publish Error [${topic}]:`, err.message);
        else     console.log(`📤 MQTT → [${topic}] : [${payload}]`);
    });
}

// ─────────────────────────────────────────────
//  3. Data Seeding
// ─────────────────────────────────────────────
async function seedDatabase() {
    const H              = 'HOUSE1';
//    const pinDoor        = await bcrypt.hash('1234', 10);
    const pinApartment   = await bcrypt.hash('0000', 10);

    const rooms = [
        { name: 'Living Room', key: 'living',   houseCode: H },
        { name: 'Bed Room',    key: 'bedroom',  houseCode: H },
        { name: 'Bath Room',   key: 'bathroom', houseCode: H },
        { name: 'Kitchen',     key: 'kitchen',  houseCode: H },
        { name: 'Kids Room',   key: 'kidsroom', houseCode: H },
        { name: 'Storage',     key: 'storage',  houseCode: H },
        { name: 'Hallway',     key: 'hallway',  houseCode: H },
        { name: 'Garage',      key: 'garage',   houseCode: H },
    ];

    const devices = [
        // Living Room
        { name: 'Light1',        type: 'light',  roomKey: 'living',   houseCode: H },
        { name: 'Light2',        type: 'light',  roomKey: 'living',   houseCode: H },
        { name: 'Fan',           type: 'fan',    roomKey: 'living',   houseCode: H, value: 0, status: false },
        { name: 'Motion Sensor', type: 'sensor', roomKey: 'living',   houseCode: H },
        { name: 'Temperature',   type: 'sensor', roomKey: 'living',   houseCode: H },
        // Bedroom
        { name: 'Light',         type: 'light',  roomKey: 'bedroom',  houseCode: H },
        { name: 'Fan',           type: 'fan',    roomKey: 'bedroom',  houseCode: H, value: 0, status: false },
        { name: 'Temperature',   type: 'sensor', roomKey: 'bedroom',  houseCode: H },
        // Bathroom
        { name: 'Light',         type: 'light',  roomKey: 'bathroom', houseCode: H },
        { name: 'Gas',           type: 'sensor', roomKey: 'bathroom', houseCode: H },
        // Kitchen
        { name: 'Light',         type: 'light',  roomKey: 'kitchen',  houseCode: H },
        { name: 'Temperature',   type: 'sensor', roomKey: 'kitchen',  houseCode: H },
        { name: 'Gas Sensor',    type: 'sensor', roomKey: 'kitchen',  houseCode: H },
        // Kids Room
        { name: 'Light',         type: 'light',  roomKey: 'kidsroom', houseCode: H },
        { name: 'Fan',           type: 'fan',    roomKey: 'kidsroom', houseCode: H, value: 0, status: false },
        // Storage
        { name: 'Light',         type: 'light',  roomKey: 'storage',  houseCode: H },
        { name: 'Fan',           type: 'fan',    roomKey: 'storage',  houseCode: H, value: 0, status: false },
        // Garage
        { name: 'Light',         type: 'light',  roomKey: 'garage',   houseCode: H },
        { name: 'GarageDoor',    type: 'door',   roomKey: 'garage',   houseCode: H },
        // Hallway
        { name: 'Light1',        type: 'light',  roomKey: 'hallway',  houseCode: H },
        { name: 'Light2',        type: 'light',  roomKey: 'hallway',  houseCode: H },
        { name: 'ApartmentDoor', type: 'door',   roomKey: 'hallway',  houseCode: H, pinCode: pinApartment, status: false },
    ];

    try {
        for (const r of rooms) {
            await Room.findOneAndUpdate(
                { key: r.key, houseCode: r.houseCode }, r,
                { upsert: true, new: true }
            );
        }
        for (const d of devices) {
            await Device.findOneAndUpdate(
                { name: d.name, roomKey: d.roomKey, houseCode: d.houseCode },
                { $setOnInsert: d },
                { upsert: true, new: true }
            );
        }
        console.log('🏠 Home structure is ready');
    } catch (err) {
        console.error('❌ Seeding error:', err.message);
    }
}

// ─────────────────────────────────────────────
//  4. Rate Limiters
// ─────────────────────────────────────────────
const loginLimiter = rateLimit({
    windowMs: 3 * 60 * 1000, max: 10,
    message:        { error: 'Too many login attempts, try again after 3 minutes.' },
    standardHeaders: true,
    legacyHeaders:   false
});

const generalLimiter = rateLimit({
    windowMs: 60 * 1000, max: 100,
    message: { error: 'Too many requests, slow down.' }
});

app.use('/api/', generalLimiter);

// ─────────────────────────────────────────────
//  5. Auth Middleware
// ─────────────────────────────────────────────
const authMiddleware = (req, res, next) => {
    const header = req.headers.authorization;
    if (!header?.startsWith('Bearer '))
        return res.status(401).json({ error: 'Access Denied: No Token Provided' });
    try {
        const payload = jwt.verify(header.split(' ')[1], process.env.JWT_SECRET);
        if (!payload.id || !payload.houseCode)
            return res.status(401).json({ error: 'Invalid Token Payload' });
        req.user = payload;
        next();
    } catch {
        res.status(401).json({ error: 'Invalid or Expired Token' });
    }
};

const hardwareAuth = (req, res, next) => {
    if (req.headers['x-api-key'] !== process.env.HARDWARE_API_KEY)
        return res.status(401).json({ error: 'Unauthorized: Invalid API Key' });
    next();
};

// ─────────────────────────────────────────────
//  6. Health Check
// ─────────────────────────────────────────────
app.get('/health', (req, res) => {
    res.json({
        status:   'ok',
        mqtt:     mqttClient?.connected ? 'connected' : 'disconnected',
        db:       mongoose.connection.readyState === 1 ? 'connected' : 'disconnected',
        uptime:   process.uptime()
    });
});

// ─────────────────────────────────────────────
//  7. API Routes
// ─────────────────────────────────────────────

// --- Auth ---
app.post('/api/register', async (req, res) => {
    try {
        const { fullName, email, password, confirm_password, houseCode } = req.body;
        if (!fullName || !email || !password || !confirm_password || !houseCode)
            return res.status(400).json({ error: 'Please complete all required fields' });
        if (password !== confirm_password)
            return res.status(400).json({ error: 'Passwords do not match' });
        if (password.length < 6)
            return res.status(400).json({ error: 'Password must be at least 6 characters' });
        if (await User.findOne({ email: email.toLowerCase() }))
            return res.status(409).json({ error: 'Email already registered' });

        const user = await User.create({
            fullName, email, houseCode,
            password: await bcrypt.hash(password, 10)
        });
        const token = jwt.sign(
            { id: user._id, houseCode: user.houseCode },
            process.env.JWT_SECRET,
            { expiresIn: '7d' }
        );
        res.status(201).json({ message: 'User created successfully', token });
    } catch (err) {
        console.error('Register error:', err.message);
        res.status(500).json({ error: 'Error creating user' });
    }
});

app.post('/api/login', loginLimiter, async (req, res) => {
    try {
        const { email, password } = req.body;
        if (!email || !password)
            return res.status(400).json({ error: 'Email and password required' });

        const user = await User.findOne({ email: email.toLowerCase() });
        if (!user || !(await bcrypt.compare(password, user.password)))
            return res.status(401).json({ error: 'Invalid email or password' });

        const token = jwt.sign(
            { id: user._id, houseCode: user.houseCode },
            process.env.JWT_SECRET,
            { expiresIn: '7d' }
        );
        res.json({ token, user: { fullName: user.fullName, email: user.email, houseCode: user.houseCode } });
    } catch (err) {
        console.error('Login error:', err.message);
        res.status(500).json({ error: 'Login error' });
    }
});

// --- Rooms & Devices ---
app.get('/api/rooms', authMiddleware, async (req, res) => {
    try {
        res.json(await Room.find({ houseCode: req.user.houseCode }));
    } catch { res.status(500).json({ error: 'Failed to fetch rooms' }); }
});

app.get('/api/rooms/:roomKey/devices', authMiddleware, async (req, res) => {
    try {
        const devices = await Device.find({
            roomKey:   req.params.roomKey,
            houseCode: req.user.houseCode
        }).select('-pinCode'); // ✅ لا نرسل الـ pinCode للـ client
        res.json(devices);
    } catch { res.status(500).json({ error: 'Failed to fetch devices' }); }
});

app.patch('/api/devices/:id', authMiddleware, async (req, res) => {
    try {
        const { status, value } = req.body;
        const updateData = {};

        if (status !== undefined) updateData.status = status;
        if (value  !== undefined) {
            updateData.value  = value;
            updateData.status = value > 0;
        }

        const device = await Device.findOneAndUpdate(
            { _id: req.params.id, houseCode: req.user.houseCode },
            updateData,
            { new: true }
        );
        if (!device) return res.status(404).json({ error: 'Device not found' });

        await Log.create({
            sensorName:  device.name,
            value:       device.value,
            roomKey:     device.roomKey,
            houseCode:   req.user.houseCode,
            eventType:   'control',
            triggeredBy: req.user.id
        });

        const topic   = `technohome/${req.user.houseCode}/${device.roomKey}/${device.name}`;
        const payload = device.type === 'fan'
            ? String(device.value)
            : (device.status ? 'ON' : 'OFF');
        mqttPublish(topic, payload);

        io.to(req.user.houseCode).emit('device_updated', device);
        res.json(device);
    } catch (err) {
        console.error('Device update error:', err.message);
        res.status(500).json({ error: 'Error updating device' });
    }
});

// --- Sensor Update (from ESP via HTTP) ---
app.post('/api/sensor/update', hardwareAuth, async (req, res) => {
    try {
        const { roomKey, sensorName, value, houseCode } = req.body;
        if (!roomKey || !sensorName || value === undefined || !houseCode)
            return res.status(400).json({ error: 'Missing required fields' });

        // ✅ FIX: استخدام { new: true } بدل returnDocument
        const device = await Device.findOneAndUpdate(
            { roomKey, name: sensorName, houseCode },
            { value },
            { new: true }
        );
        if (!device) return res.status(404).json({ error: 'Sensor not found' });

        await Log.create({ sensorName, value, roomKey, houseCode, eventType: 'sensor' });

        _emitSensorAlerts(io, houseCode, roomKey, sensorName, value);
        io.to(houseCode).emit('update_ui', { roomKey, sensorName, value });
        res.json({ success: true });
    } catch (err) {
        console.error('Sensor update error:', err.message);
        res.status(500).json({ error: 'Server error' });
    }
});

// --- Door ---
app.post('/api/devices/unlock-door', authMiddleware, async (req, res) => {
    try {
        const { deviceId, pin } = req.body;
        if (!deviceId || !pin)
            return res.status(400).json({ error: 'deviceId and pin are required' });

        const device = await Device.findOne({ _id: deviceId, houseCode: req.user.houseCode });
        if (!device || device.type !== 'door')
            return res.status(404).json({ error: 'Door not found' });

        if (!(await bcrypt.compare(pin, device.pinCode))) {
            await Log.create({
                sensorName: device.name, value: 0, roomKey: device.roomKey,
                houseCode: req.user.houseCode, eventType: 'door', triggeredBy: req.user.id
            });
            return res.status(401).json({ success: false, message: 'Wrong PIN Code' });
        }

        device.status = true;
        await device.save();

        await Log.create({
            sensorName: device.name, value: 1, roomKey: device.roomKey,
            houseCode: req.user.houseCode, eventType: 'door', triggeredBy: req.user.id
        });

        const topic = `technohome/${req.user.houseCode}/${device.roomKey}/${device.name}`;
        console.log(`🔓 Unlocking door → topic: [${topic}] | MQTT: ${mqttClient?.connected}`);
        mqttPublish(topic, 'UNLOCK');

        io.to(req.user.houseCode).emit('device_updated', { ...device.toObject(), pinCode: undefined });
        console.log(`✅ Door [${device.name}] Unlocked`);

        // Auto-lock بعد 5 ثواني
        setTimeout(async () => {
            try {
                device.status = false;
                await device.save();
                mqttPublish(topic, 'LOCK');
                io.to(req.user.houseCode).emit('device_updated', { ...device.toObject(), pinCode: undefined });
                console.log('🔒 Door Auto-Locked');
            } catch (e) {
                console.error('Auto-lock error:', e.message);
            }
        }, 5000);

        res.json({ success: true, message: 'Door Unlocked' });
    } catch (err) {
        console.error('Unlock door error:', err.message);
        res.status(500).json({ error: 'Server Error' });
    }
});

app.patch('/api/devices/:id/change-pin', authMiddleware, async (req, res) => {
    try {
        const { oldPin, newPin } = req.body;
        if (!oldPin || !newPin)
            return res.status(400).json({ error: 'Required fields missing' });
        if (newPin.length < 4)
            return res.status(400).json({ error: 'PIN must be at least 4 digits' });

        const device = await Device.findOne({ _id: req.params.id, houseCode: req.user.houseCode });
        if (!device || device.type !== 'door')
            return res.status(404).json({ error: 'Door not found' });
        if (!(await bcrypt.compare(oldPin, device.pinCode)))
            return res.status(401).json({ error: 'Old PIN is incorrect' });

        device.pinCode = await bcrypt.hash(newPin, 10);
        await device.save();
        res.json({ success: true, message: 'PIN updated successfully' });
    } catch (err) {
        console.error('Change PIN error:', err.message);
        res.status(500).json({ error: 'Server Error' });
    }
});

// --- Family & Profile ---
app.get('/api/family', authMiddleware, async (req, res) => {
    try {
        const members = await User.find({ houseCode: req.user.houseCode }).select('fullName email');
        res.json({ count: members.length, members });
    } catch { res.status(500).json({ error: 'Server error' }); }
});

app.patch('/api/profile', authMiddleware, async (req, res) => {
    try {
        const { fullName, currentPassword, newPassword } = req.body;
        const user = await User.findById(req.user.id);
        if (!user) return res.status(404).json({ error: 'User not found' });

        const updateData = {};
        if (fullName?.trim()) updateData.fullName = fullName.trim();

        if (newPassword) {
            if (!currentPassword)
                return res.status(400).json({ error: 'Current password is required' });
            if (newPassword.length < 6)
                return res.status(400).json({ error: 'Password must be at least 6 characters' });
            if (!(await bcrypt.compare(currentPassword, user.password)))
                return res.status(401).json({ error: 'Current password is incorrect' });
            updateData.password = await bcrypt.hash(newPassword, 10);
        }

        if (!Object.keys(updateData).length)
            return res.status(400).json({ error: 'No data provided' });

        const updated = await User.findByIdAndUpdate(req.user.id, updateData, { new: true })
            .select('fullName email houseCode');
        res.json({ success: true, user: updated });
    } catch (err) {
        console.error('Profile update error:', err.message);
        res.status(500).json({ error: 'Server Error' });
    }
});

// --- Schedules ---
app.get('/api/schedules', authMiddleware, async (req, res) => {
    try {
       res.json(await Schedule.find({ houseCode: req.user.houseCode })
            .populate('deviceId', 'name roomKey type'));
    } catch { res.status(500).json({ error: 'Failed' }); }
});

app.post('/api/schedules', authMiddleware, async (req, res) => {
    try {
        const { deviceId, action, cronTime, days, label } = req.body;
        if (!await Device.findOne({ _id: deviceId, houseCode: req.user.houseCode }))
            return res.status(404).json({ error: 'Device not found' });

        const schedule = await Schedule.create({
            houseCode: req.user.houseCode, deviceId, action, cronTime, label,
            days: days || ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        });
        res.status(201).json(schedule);
    } catch { res.status(500).json({ error: 'Failed' }); }
});

app.delete('/api/schedules/:id', authMiddleware, async (req, res) => {
    try {
        await Schedule.findOneAndDelete({ _id: req.params.id, houseCode: req.user.houseCode });
        res.json({ success: true });
    } catch { res.status(500).json({ error: 'Failed' }); }
});

// --- Logs ---
app.get('/api/logs', authMiddleware, async (req, res) => {
    try {
        const { roomKey, eventType, limit = 50 } = req.query;
        const filter = { houseCode: req.user.houseCode };
        if (roomKey)   filter.roomKey   = roomKey;
        if (eventType) filter.eventType = eventType;

        const logs = await Log.find(filter)
            .sort({ timestamp: -1 })
            .limit(Math.min(parseInt(limit) || 50, 200)); // ✅ max 200
        res.json(logs);
    } catch { res.status(500).json({ error: 'Failed' }); }
});

// ─────────────────────────────────────────────
//  8. Socket.IO
// ─────────────────────────────────────────────
io.on('connection', (socket) => {
    const houseCode = socket.handshake.query.houseCode;
    if (houseCode) {
        socket.join(houseCode);
        console.log(`🔌 Socket joined house: ${houseCode} | id: ${socket.id}`);
    }
    socket.on('disconnect', () => {
        console.log(`❌ Socket disconnected: ${socket.id}`);
    });
});


// ─────────────────────────────────────────────
//  9b. MQTT Door Command Handler
// ─────────────────────────────────────────────
// تست من MQTTX:
//   Topic:   technohome/HOUSE1/hallway/door/command
//   Payload: {"doorName":"ApartmentDoor","pin":"0000"}
function handleMqttDoorCommand(topic, message) {
    const payload = message.toString();
    const parts   = topic.split('/');

    // format: technohome/{houseCode}/{roomKey}/door/command
    if (parts.length !== 5 ||
        parts[0] !== 'technohome' ||
        parts[3] !== 'door'      ||
        parts[4] !== 'command') return;

    const houseCode = parts[1];
    const roomKey   = parts[2];

    (async () => {
        try {
            const { doorName, pin } = JSON.parse(payload);
            if (!doorName || !pin) {
                console.warn('⚠️  [MQTT Door] Missing doorName or pin');
                return;
            }

            console.log(`🔑 [MQTT Door] Request: ${doorName} | house: ${houseCode}`);

            const device = await Device.findOne({ name: doorName, roomKey, houseCode, type: 'door' });
            if (!device) {
                console.warn(`⚠️  [MQTT Door] Door not found: ${doorName}`);
                return;
            }

            // التحقق من الـ PIN
            const pinMatch = await bcrypt.compare(pin, device.pinCode);
            if (!pinMatch) {
                console.warn(`❌ [MQTT Door] Wrong PIN for: ${doorName}`);
                await Log.create({
                    sensorName: device.name, value: 0,
                    roomKey, houseCode, eventType: 'door', triggeredBy: 'mqtt'
                });
                // ابعت رد للـ MQTTX
                mqttPublish(`technohome/${houseCode}/${roomKey}/door/response`,
                    JSON.stringify({ doorName, success: false, message: 'Wrong PIN' }));
                return;
            }

            // فتح الباب
            device.status = true;
            await device.save();

            await Log.create({
                sensorName: device.name, value: 1,
                roomKey, houseCode, eventType: 'door', triggeredBy: 'mqtt'
            });

            const doorTopic = `technohome/${houseCode}/${roomKey}/${device.name}`;
            mqttPublish(doorTopic, 'UNLOCK');
            io.to(houseCode).emit('device_updated', { ...device.toObject(), pinCode: undefined });

            // رد للـ MQTTX
            mqttPublish(`technohome/${houseCode}/${roomKey}/door/response`,
                JSON.stringify({ doorName, success: true, message: 'Door Unlocked' }));

            console.log(`✅ [MQTT Door] ${doorName} Unlocked`);

            // Auto-lock بعد 5 ثواني
            setTimeout(async () => {
                try {
                    device.status = false;
                    await device.save();
                    mqttPublish(doorTopic, 'LOCK');
                    io.to(houseCode).emit('device_updated', { ...device.toObject(), pinCode: undefined });
                    mqttPublish(`technohome/${houseCode}/${roomKey}/door/response`,
                        JSON.stringify({ doorName, success: true, message: 'Door Auto-Locked' }));
                    console.log(`🔒 [MQTT Door] ${doorName} Auto-Locked`);
                } catch (e) {
                    console.error('Auto-lock error:', e.message);
                }
            }, 5000);

        } catch (err) {
            console.error(`❌ [MQTT Door] Error on [${topic}]:`, err.message);
        }
    })();
}

// ─────────────────────────────────────────────
//  9. MQTT Handler — كل الغرف
// ─────────────────────────────────────────────
function handleMqttMessage(topic, message) {
    const payload    = message.toString();
    const parts      = topic.split('/');

    // format: technohome/{houseCode}/{roomKey}/sensor/update
    if (parts.length !== 5 ||
        parts[0] !== 'technohome' ||
        parts[3] !== 'sensor'    ||
        parts[4] !== 'update') return;

    const houseCode = parts[1];
    const roomKey   = parts[2];

    (async () => {
        try {
            const { sensorName, value } = JSON.parse(payload);
            if (!sensorName || value === undefined) return;

            // 1. حدّث الـ DB
            const device = await Device.findOneAndUpdate(
                { name: sensorName, roomKey, houseCode },
                { value },
                { new: true }
            );
            if (!device) {
                console.warn(`⚠️  Device not found: ${sensorName} | ${roomKey} | ${houseCode}`);
                return;
            }

            // 2. سجّل الحدث
            await Log.create({
                sensorName, value, roomKey, houseCode,
                eventType: 'sensor', triggeredBy: 'hardware'
            });

            // 3. حدّث الموبايل
            io.to(houseCode).emit('update_ui', { roomKey, sensorName, value });

            // 4. إنذارات
            _emitSensorAlerts(io, houseCode, roomKey, sensorName, value);

            console.log(`✅ [MQTT] ${houseCode}/${roomKey}/${sensorName} = ${value}`);
        } catch (err) {
            console.error(`❌ [MQTT] Parse error on [${topic}]:`, err.message);
        }
    })();
}

// ─────────────────────────────────────────────
//  10. Sensor Alerts Helper
// ─────────────────────────────────────────────
function _emitSensorAlerts(io, houseCode, roomKey, sensorName, value) {
    const name = sensorName.toLowerCase();

    if (sensorName === 'Intruder Alert' && value === 1) {
        io.to(houseCode).emit('danger_alert', {
            type: 'SECURITY', roomKey, value: 1,
            message: '⚠️ تنبيه: تم رصد محاولة اختراق للباب!'
        });
    }
    if (name.includes('gas') && value > 400) {
        io.to(houseCode).emit('danger_alert', {
            type: 'GAS', roomKey, value,
            message: `⚠️ Gas leak detected in ${roomKey}! Value: ${value}`
        });
    }
    if (name.includes('temperature') && value > 45) {
        io.to(houseCode).emit('danger_alert', {
            type: 'TEMPERATURE', roomKey, value,
            message: `⚠️ High temperature in ${roomKey}! Temp: ${value}°C`
        });
    }
}

// ─────────────────────────────────────────────
//  11. Start Server
// ─────────────────────────────────────────────
async function startServer() {
    try {
        // MongoDB Atlas
        if (!process.env.MONGO_URI) {
            console.error('❌ MONGO_URI is not defined in .env');
            process.exit(1);
        }
        await mongoose.connect(process.env.MONGO_URI, {
            serverSelectionTimeoutMS: 10000
        });
        console.log('✅ Connected to MongoDB');
        await seedDatabase();

        // HiveMQ Cloud
        if (!process.env.HIVEMQ_HOST || !process.env.HIVEMQ_USER || !process.env.HIVEMQ_PASSWORD) {
            console.error('❌ HiveMQ credentials missing in .env');
            process.exit(1);
        }

        mqttClient = mqtt.connect(`mqtts://${process.env.HIVEMQ_HOST}:8883`, {
            username:            process.env.HIVEMQ_USER,
            password:            process.env.HIVEMQ_PASSWORD,
            clientId:            `tecnohouse-server-${Date.now()}`,
            rejectUnauthorized:  true,
            reconnectPeriod:     5000,
            connectTimeout:      30000,
            // ✅ تأكيد استلام الرسائل QoS 1
            clean: false
        });

        mqttClient.on('connect', () => {
            console.log(`✅ Connected to HiveMQ: ${process.env.HIVEMQ_HOST}`);

            // اشتراك 1: قراءات الحساسات من الـ ESP
            mqttClient.subscribe('technohome/+/+/sensor/update', { qos: 1 }, (err) => {
                if (err) console.error('❌ Subscribe error:', err.message);
                else     console.log('📡 Subscribed to: technohome/+/+/sensor/update');
            });

            // اشتراك 2: أوامر الأبواب عن طريق MQTT مباشرة
            // format: technohome/{houseCode}/{roomKey}/door/command
            // payload: {"doorName":"ApartmentDoor","pin":"0000"}
            mqttClient.subscribe('technohome/+/+/door/command', { qos: 1 }, (err) => {
                if (err) console.error('❌ Subscribe error:', err.message);
                else     console.log('📡 Subscribed to: technohome/+/+/door/command');
            });
        });

        mqttClient.on('message', (topic, message) => {
            handleMqttMessage(topic, message);
            handleMqttDoorCommand(topic, message);

        });
        mqttClient.on('reconnect',  ()    => console.log('🔄 MQTT Reconnecting...'));
        mqttClient.on('error',      (err) => console.error('❌ MQTT Error:', err.message));
        mqttClient.on('offline',    ()    => console.warn('⚠️  MQTT Offline'));
        mqttClient.on('disconnect', ()    => console.warn('⚠️  MQTT Disconnected from broker'));

        // HTTP Server
        server.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));

    } catch (err) {
        console.error('❌ Failed to start server:', err.message);
        process.exit(1);
    }
}

// ─────────────────────────────────────────────
//  12. Graceful Shutdown
// ─────────────────────────────────────────────
async function gracefulShutdown(signal) {
    console.log(`\n⏹️  Received ${signal} — shutting down gracefully...`);
    try {
        server.close();
        mqttClient?.end(true);
        await mongoose.connection.close();
        console.log('✅ Shutdown complete');
        process.exit(0);
    } catch (err) {
        console.error('❌ Shutdown error:', err.message);
        process.exit(1);
    }
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));

process.on('unhandledRejection', (reason) => {
    console.error('❌ Unhandled Rejection:', reason);
});

startServer();

require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const bcrypt = require('bcrypt');
const rateLimit = require('express-rate-limit');

const { Aedes } = require('aedes');
const net = require('net');
let aedes; 

const app = express();
app.use(express.json());
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

const MQTT_PORT = 1883;
const PORT = process.env.PORT || 3000;

// ================== 1. Models ==================
const userSchema = new mongoose.Schema({
    fullName: { type: String, required: true },
    email: { type: String, required: true, unique: true },
    password: { type: String, required: true },
    houseCode: { type: String, required: true },
    settings: {
        notifications: { type: Boolean, default: true },
        darkMode: { type: Boolean, default: false }
    }
});

const logSchema = new mongoose.Schema({
    sensorName: { type: String, required: true },
    value: { type: Number },
    roomKey: String,
    houseCode: { type: String, required: true },
    eventType: { type: String, enum: ['sensor', 'control', 'door'], default: 'sensor' },
    triggeredBy: { type: String, default: 'system' },
    timestamp: { type: Date, default: Date.now }
});

const roomSchema = new mongoose.Schema({
    name: { type: String, required: true },
    key: { type: String, required: true },
    houseCode: { type: String, required: true }
});

const deviceSchema = new mongoose.Schema({
    name: { type: String, required: true },
    type: { type: String, required: true },
    roomKey: { type: String, required: true },
    houseCode: { type: String, required: true },
    status: { type: Boolean, default: false },
    value: { type: Number, default: 0 },
    pinCode: { type: String, required: true }
});

const scheduleSchema = new mongoose.Schema({
    houseCode: { type: String, required: true },
    deviceId: { type: mongoose.Schema.Types.ObjectId, ref: 'Device', required: true },
    action: { status: Boolean, value: Number },
    cronTime: { type: String, required: true },
    days: [{ type: String }],
    isActive: { type: Boolean, default: true },
    label: { type: String }
});

const User = mongoose.model('User', userSchema);
const Log = mongoose.model('Log', logSchema);
const Room = mongoose.model('Room', roomSchema);
const Device = mongoose.model('Device', deviceSchema);
const Schedule = mongoose.model('Schedule', scheduleSchema);

// ================== 2. Data Seeding ==================
async function seedDatabase() {
    const defaultHouse = "HOUSE1";

    const rooms = [
        { name: 'Living Room', key: 'living', houseCode: defaultHouse },
        { name: 'Bed Room', key: 'bedroom', houseCode: defaultHouse },
        { name: 'Bath Room', key: 'bathroom', houseCode: defaultHouse },
        { name: 'Kitchen', key: 'kitchen', houseCode: defaultHouse },
        { name: 'Kids Room', key: 'kidsroom', houseCode: defaultHouse },
        { name: 'Storage', key: 'storage', houseCode: defaultHouse },
        { name: 'Hallway', key: 'hallway', houseCode: defaultHouse },
        { name: 'Garage', key: 'garage', houseCode: defaultHouse }
    ];

    const defaultPinHash = await bcrypt.hash("1234", 10);
    const apartmentPinHash = await bcrypt.hash("0000", 10);

    const devices = [
        { name: 'Light1', type: 'light', roomKey: 'living', houseCode: defaultHouse },
        { name: 'Light2', type: 'light', roomKey: 'living', houseCode: defaultHouse },
        { name: 'Fan', type: 'fan', roomKey: 'living', value: 0, status: false, houseCode: defaultHouse },
        { name: 'Motion Sensor', type: 'sensor', roomKey: 'living', houseCode: defaultHouse },
        { name: 'Temperature', type: 'sensor', roomKey: 'living', houseCode: defaultHouse },
        { name: 'Light', type: 'light', roomKey: 'bedroom', houseCode: defaultHouse },
        { name: 'Fan', type: 'fan', roomKey: 'bedroom', value: 0, status: false, houseCode: defaultHouse },
        { name: 'Temperature', type: 'sensor', roomKey: 'bedroom', houseCode: defaultHouse },
        { name: 'Light', type: 'light', roomKey: 'bathroom', houseCode: defaultHouse },
        { name: 'Gas', type: 'sensor', roomKey: 'bathroom', houseCode: defaultHouse },
        { name: 'Light', type: 'light', roomKey: 'kitchen', houseCode: defaultHouse },
        { name: 'Temperature', type: 'sensor', roomKey: 'kitchen', houseCode: defaultHouse },
        { name: 'Gas Sensor', type: 'sensor', roomKey: 'kitchen', houseCode: defaultHouse },
        { name: 'Light', type: 'light', roomKey: 'kidsroom', houseCode: defaultHouse },
        { name: 'Fan', type: 'fan', roomKey: 'kidsroom', value: 0, status: false, houseCode: defaultHouse },
        { name: 'Light', type: 'light', roomKey: 'storage', houseCode: defaultHouse },
        { name: 'Fan', type: 'fan', roomKey: 'storage', value: 0, status: false, houseCode: defaultHouse },
        { name: 'Light', type: 'light', roomKey: 'garage', houseCode: defaultHouse },
        { name: 'GarageDoor', type: 'door', roomKey: 'garage', houseCode: defaultHouse, pinCode: defaultPinHash },
        { name: 'Light1', type: 'light', roomKey: 'hallway', houseCode: defaultHouse },
        { name: 'Light2', type: 'light', roomKey: 'hallway', houseCode: defaultHouse },
        { name: 'ApartmentDoor', type: 'door', roomKey: 'hallway', houseCode: defaultHouse, pinCode: apartmentPinHash, status: false }
    ];

    try {
        for (let r of rooms) {
            await Room.findOneAndUpdate({ key: r.key, houseCode: r.houseCode }, r, { upsert: true, returnDocument: 'after' });
        }
        for (let d of devices) {
            await Device.findOneAndUpdate(
                { name: d.name, roomKey: d.roomKey, houseCode: d.houseCode },
                { $setOnInsert: d },
                { upsert: true, returnDocument: 'after' }
            );
        }
        console.log('🏠 Home structure is ready');
    } catch (err) {
        console.error('Error seeding data:', err);
    }
}

// ================== 3. Middleware ==================
const loginLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 10,
    message: { error: 'Too many login attempts, please try again after 15 minutes.' },
    standardHeaders: true,
    legacyHeaders: false
});

const generalLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 100,
    message: { error: 'Too many requests, slow down.' }
});
app.use('/api/', generalLimiter);

const authMiddleware = (req, res, next) => {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Access Denied: No Token Provided' });
    }

    const token = authHeader.split(' ')[1];
    try {
        const verified = jwt.verify(token, process.env.JWT_SECRET);
        if (!verified.id || !verified.houseCode) {
            return res.status(401).json({ error: 'Access Denied: Invalid Token Payload' });
        }
        req.user = verified;
        next();
    } catch (err) {
        res.status(400).json({ error: 'Invalid Token' });
    }
};

const hardwareAuthMiddleware = (req, res, next) => {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey || apiKey !== process.env.HARDWARE_API_KEY) {
        return res.status(401).json({ error: 'Unauthorized: Invalid API Key' });
    }
    next();
};

// ================== 4. APIs ==================
app.post('/api/register', async (req, res) => {
    try {
        const { fullName, email, password, confirm_password, houseCode } = req.body;
        if (!fullName || !password || !email || !confirm_password || !houseCode) {
            return res.status(400).json({ error: 'Please complete all required fields' });
        }
        if (password !== confirm_password) {
            return res.status(400).json({ error: 'Passwords do not match' });
        }
        const existingUser = await User.findOne({ email });
        if (existingUser) return res.status(400).json({ error: 'Wrong in registered' });

        const hashed = await bcrypt.hash(password, 10);
        const user = new User({ fullName, email, password: hashed, houseCode });
        await user.save();

        const token = jwt.sign({ id: user._id, houseCode: user.houseCode }, process.env.JWT_SECRET, { expiresIn: '7d' });
        res.status(201).json({ message: 'User created successfully', token });
    } catch (err) {
        res.status(500).json({ error: 'Error creating user' });
    }
});

app.post('/api/login',  async (req, res) => {
    try {
        const { email, password } = req.body;
        const user = await User.findOne({ email });
        if (!user) return res.status(401).json({ error: 'Invalid email or password' });

        const valid = await bcrypt.compare(password, user.password);
        if (!valid) return res.status(401).json({ error: 'Invalid email or password' });

        const token = jwt.sign({ id: user._id, houseCode: user.houseCode }, process.env.JWT_SECRET, { expiresIn: '7d' });
        res.json({ token, user: { fullName: user.fullName, email: user.email, houseCode: user.houseCode } });
    } catch (err) {
        res.status(500).json({ error: 'Login error' });
    }
});

app.get('/api/rooms', authMiddleware, async (req, res) => {
    try {
        const rooms = await Room.find({ houseCode: req.user.houseCode });
        res.json(rooms);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch rooms' });
    }
});

app.get('/api/rooms/:roomKey/devices', authMiddleware, async (req, res) => {
    try {
        const devices = await Device.find({ roomKey: req.params.roomKey, houseCode: req.user.houseCode });
        res.json(devices);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch devices' });
    }
});

app.patch('/api/devices/:id', authMiddleware, async (req, res) => {
    try {
        const { status, value } = req.body;
        const updateData = { status, value };

        if (value !== undefined) {
            updateData.status = value > 0;
        }

        const device = await Device.findOneAndUpdate(
            { _id: req.params.id, houseCode: req.user.houseCode },
            updateData,
            { new: true }
        );

        if (!device) return res.status(404).json({ error: 'Device not found' });

        await Log.create({
            sensorName: device.name,
            value: device.value,
            roomKey: device.roomKey,
            houseCode: req.user.houseCode,
            eventType: 'control',
            triggeredBy: req.user.id
        });

        if (aedes) {
            const mqttTopic = `technohome/${req.user.houseCode}/${device.roomKey}/${device.name}`;
            const mqttPayload = device.type === 'fan' ? String(device.value) : (device.status ? "ON" : "OFF");

            aedes.publish({
                topic: mqttTopic,
                payload: Buffer.from(mqttPayload),
                qos: 0,
                retain: false
            });

            console.log(`📤 MQTT Published: [${mqttPayload}] to Topic: [${mqttTopic}]`);
        }

        io.to(req.user.houseCode).emit('device_updated', device);
        res.json(device);
    } catch (err) {
        res.status(500).json({ error: 'Error updating device' });
    }
});

app.post('/api/sensor/update', hardwareAuthMiddleware, async (req, res) => {
    try {
        const { roomKey, sensorName, value, houseCode } = req.body;
        if (!roomKey || !sensorName || value === undefined || !houseCode) {
            return res.status(400).json({ error: 'Missing required fields' });
        }

        const device = await Device.findOneAndUpdate(
            { roomKey, name: sensorName, houseCode },
            { value },
            { returnDocument: 'after' }
        );

        if (!device) return res.status(404).json({ error: 'Sensor not found' });

        await Log.create({ sensorName, value, roomKey, houseCode, eventType: 'sensor' });

        if ((sensorName.toLowerCase().includes('gas')) && value > 400) {
            io.to(houseCode).emit('danger_alert', {
                type: 'GAS', roomKey, value, message: `⚠️ Gas leak detected in ${roomKey}! Value: ${value}`
            });
        }

        if (sensorName.toLowerCase().includes('temperature') && value > 45) {
            io.to(houseCode).emit('danger_alert', {
                type: 'TEMPERATURE', roomKey, value, message: `⚠️ High temperature in ${roomKey}! Temp: ${value}°C`
            });
        }

        io.to(houseCode).emit('update_ui', { roomKey, sensorName, value });
        res.json({ success: true, message: 'Sensor updated' });
    } catch (err) {
        res.status(500).json({ error: 'Server error' });
    }
});

app.get('/api/family', authMiddleware, async (req, res) => {
    try {
        const familyMembers = await User.find({ houseCode: req.user.houseCode }).select('fullName email');
        res.json({ count: familyMembers.length, members: familyMembers });
    } catch (err) {
        res.status(500).json({ error: 'Server error' });
    }
});

app.patch('/api/profile', authMiddleware, async (req, res) => {
    try {
        const { fullName, currentPassword, newPassword } = req.body;
        const user = await User.findById(req.user.id);
        if (!user) return res.status(404).json({ error: 'User not found' });

        const updateData = {};
        if (fullName && fullName.trim()) updateData.fullName = fullName.trim();

        if (newPassword) {
            if (!currentPassword) return res.status(400).json({ error: 'Current password is required' });
            if (newPassword.length < 6) return res.status(400).json({ error: 'Password must be at least 6 characters' });
            const valid = await bcrypt.compare(currentPassword, user.password);
            if (!valid) return res.status(401).json({ error: 'Current password is incorrect' });
            updateData.password = await bcrypt.hash(newPassword, 10);
        }

        if (Object.keys(updateData).length === 0) return res.status(400).json({ error: 'No data provided' });

        const updatedUser = await User.findByIdAndUpdate(req.user.id, updateData, { new: true }).select('fullName email houseCode');
        res.json({ success: true, user: updatedUser });
    } catch (err) {
        res.status(500).json({ error: 'Server Error' });
    }
});

app.post('/api/devices/unlock-door', authMiddleware, async (req, res) => {
    try {
        const { deviceId, pin } = req.body;
        if (!deviceId || !pin) return res.status(400).json({ error: 'deviceId and pin are required' });

        const device = await Device.findOne({ _id: deviceId, houseCode: req.user.houseCode });
        if (!device || device.type !== 'door') return res.status(404).json({ error: 'Door not found' });

        const pinMatch = await bcrypt.compare(pin, device.pinCode);

        if (pinMatch) {
            device.status = true;
            await device.save();

            await Log.create({
                sensorName: device.name, value: 1, roomKey: device.roomKey,
                houseCode: req.user.houseCode, eventType: 'door', triggeredBy: req.user.id
            });

            if (aedes) {
                const mqttTopic = `technohome/${req.user.houseCode}/${device.roomKey}/${device.name}`;
                aedes.publish({ topic: mqttTopic, payload: Buffer.from("UNLOCK"), qos: 0, retain: false });
            }

            io.to(req.user.houseCode).emit('device_updated', device);
            console.log(`✅ Door [${device.name}] Unlocked`);

            setTimeout(async () => {
                device.status = false;
                await device.save();
                
                if (aedes) {
                    const mqttTopic = `technohome/${req.user.houseCode}/${device.roomKey}/${device.name}`;
                    aedes.publish({ topic: mqttTopic, payload: Buffer.from("LOCK"), qos: 0, retain: false });
                }
                
                io.to(req.user.houseCode).emit('device_updated', device);
                console.log(`🔒 Door Auto-Locked`);
            }, 5000);

            return res.json({ success: true, message: 'Door Unlocked' });
        } else {
            await Log.create({
                sensorName: device.name, value: 0, roomKey: device.roomKey,
                houseCode: req.user.houseCode, eventType: 'door', triggeredBy: req.user.id
            });
            return res.status(401).json({ success: false, message: 'Wrong PIN Code' });
        }
    } catch (err) {
        res.status(500).json({ error: 'Server Error' });
    }
});

app.patch('/api/devices/:id/change-pin', authMiddleware, async (req, res) => {
    try {
        const { oldPin, newPin } = req.body;
        if (!oldPin || !newPin) return res.status(400).json({ error: 'Required fields missing' });
        if (newPin.length < 4) return res.status(400).json({ error: 'PIN must be at least 4 digits' });

        const device = await Device.findOne({ _id: req.params.id, houseCode: req.user.houseCode });
        if (!device || device.type !== 'door') return res.status(404).json({ error: 'Door not found' });

        const pinMatch = await bcrypt.compare(oldPin, device.pinCode);
        if (!pinMatch) return res.status(401).json({ error: 'Old PIN is incorrect' });

        device.pinCode = await bcrypt.hash(newPin, 10);
        await device.save();
        res.json({ success: true, message: 'PIN updated successfully' });
    } catch (err) {
        res.status(500).json({ error: 'Server Error' });
    }
});

app.get('/api/schedules', authMiddleware, async (req, res) => {
    try {
        const schedules = await Schedule.find({ houseCode: req.user.houseCode }).populate('deviceId', 'name roomKey type');
        res.json(schedules);
    } catch (err) {
        res.status(500).json({ error: 'Failed' });
    }
});

app.post('/api/schedules', authMiddleware, async (req, res) => {
    try {
        const { deviceId, action, cronTime, days, label } = req.body;
        const device = await Device.findOne({ _id: deviceId, houseCode: req.user.houseCode });
        if (!device) return res.status(404).json({ error: 'Device not found' });

        const schedule = await Schedule.create({
            houseCode: req.user.houseCode, deviceId, action, cronTime, days: days || ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'], label
        });
        res.status(201).json(schedule);
    } catch (err) {
        res.status(500).json({ error: 'Failed' });
    }
});

app.delete('/api/schedules/:id', authMiddleware, async (req, res) => {
    try {
        await Schedule.findOneAndDelete({ _id: req.params.id, houseCode: req.user.houseCode });
        res.json({ success: true, message: 'Schedule deleted' });
    } catch (err) {
        res.status(500).json({ error: 'Failed' });
    }
});

app.get('/api/logs', authMiddleware, async (req, res) => {
    try {
        const { roomKey, eventType, limit = 50 } = req.query;
        const filter = { houseCode: req.user.houseCode };
        if (roomKey) filter.roomKey = roomKey;
        if (eventType) filter.eventType = eventType;

        const logs = await Log.find(filter).sort({ timestamp: -1 }).limit(parseInt(limit));
        res.json(logs);
    } catch (err) {
        res.status(500).json({ error: 'Failed' });
    }
});

// ================== 5. Socket ==================
io.on('connection', (socket) => {
    const houseCode = socket.handshake.query.houseCode;
    if (houseCode) {
        socket.join(houseCode);
        console.log(`🔌 Connected to House Room: ${houseCode}`);
    }
    socket.on('control_device', (data) => {
        io.to(data.houseCode).emit('hardware_command', data);
    });
    socket.on('disconnect', () => {
        console.log('❌ Socket disconnected');
    });
});

// ================== 6. Server Initialization ==================
async function startServer() {
    try {
        if (process.env.MONGO_URI) {
            await mongoose.connect(process.env.MONGO_URI);
            console.log('✅ Connected to MongoDB');
            await seedDatabase();
        } else {
            console.warn('⚠️ MONGO_URI is not defined in .env');
        }

        aedes = await Aedes.createBroker();
        const mqttServer = net.createServer(aedes.handle);
        
        mqttServer.listen(MQTT_PORT, () => {
            console.log(`✅ Local MQTT Broker started on port ${MQTT_PORT}`);
        });

        aedes.on('client', function (client) {
            console.log(`📱 Hardware (ESP) Connected: ${client ? client.id : client}`);
        });

        aedes.on('clientDisconnect', function (client) {
            console.log(`❌ Hardware (ESP) Disconnected: ${client ? client.id : client}`);
        });

        // ✅ استقبال قراءات الحساسات والإنذارات من الـ ESP عن طريق MQTT
        aedes.on('publish', async function (packet, client) {
            const topic = packet.topic;
            const payload = packet.payload.toString();

            // 1. إذا كانت الرسالة صادرة من السيرفر نفسه
            if (!client) {
                console.log(`📤 [MQTT Broker] Outgoing Command -> Topic: [${topic}] | Payload: [${payload}]`);
                return;
            }

            // 2. إذا كانت الرسالة قادمة من الـ ESP32
            const topicParts = topic.split('/'); 
            
            if (topicParts[0] === 'technohome' && topic.endsWith('update')) {
                try {
                    const data = JSON.parse(payload);
                    const { sensorName, value, houseCode, roomKey } = data;

                    if (!sensorName || value === undefined || !houseCode || !roomKey) return;

                    // 1. تحديث واجهة الموبايل فوراً عبر السوكيت
                    io.to(houseCode).emit('update_ui', { roomKey, sensorName, value });

                    // 2. تسجيل الحدث في الداتابيز
                    await Log.create({
                        sensorName, 
                        value, 
                        roomKey, 
                        houseCode, 
                        eventType: sensorName.toLowerCase().includes('door') ? 'door' : 'sensor', 
                        triggeredBy: 'hardware'
                    });

                    // 3. تحديث حالة الجهاز أو الباب في الداتابيز
                    await Device.findOneAndUpdate(
                        { name: sensorName, roomKey, houseCode },
                        { status: value === 1 || value === "ON", value: Number(value) || 0 }
                    );

                    // 4. إرسال إنذار خطر للموبايل في حالة الاختراق (بشكل ديناميكي)
                    if (sensorName === 'Intruder Alert' && (value === 1 || value === "ON")) {
                        io.to(houseCode).emit('danger_alert', {
                            type: 'SECURITY', roomKey: "hallway", value: 1, message: '⚠️ تنبيه: تم رصد محاولة اختراق للباب!'
                        });
                    }

                    console.log(`✅ [MQTT] Sensor Update: ${sensorName} = ${value}`);
                } catch (err) {
                    console.error("❌ MQTT Parse Error:", err.message);
                }
            }
        });

        // تشغيل السيرفر الأساسي (الوحيد)
        server.listen(PORT, () => {
            console.log(`🚀 Server listening on port ${PORT}`);
        });

    } catch (error) {
        console.error('❌ Failed to start server:', error);
        process.exit(1);
    }
}

startServer();

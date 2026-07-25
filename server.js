import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import axios from "axios";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";
import fs from "fs";
import path from "path";
import mongoose from "mongoose";
import QRCode from "qrcode";

dotenv.config();

const app = express();
const PORT = 3000;
const JWT_SECRET = process.env.JWT_SECRET || "super-secret-key";

app.use(cors());
app.use(express.json({ limit: "100mb" }));
app.use(express.urlencoded({ limit: "100mb", extended: true }));

// --- REAL MONGODB DATABASE CONFIGURATION ---
const userSchema = new mongoose.Schema({
  email: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  name: { type: String, default: "" },
  role: { type: String, enum: ["user", "admin"], default: "user" },
  coins: { type: Number, default: 200 },
  subscribedApis: { type: [String], default: [] },
  subscribedPlan: { type: String, default: "" }
}, { timestamps: true });

const apiSchema = new mongoose.Schema({
  name: { type: String, required: true },
  key: { type: String, default: "" },
  description: { type: String, default: "" },
  endpoint: { type: String, default: "" },
  coinsPerCall: { type: Number, default: 0 },
  subscriptionPrice: { type: Number, default: 0 },
  type: { type: String, default: "API" },
  category: { type: String, default: "Text" },
  curlTemplate: { type: String, default: "" },
  published: { type: Boolean, default: false },
  planBatch: { type: String, default: "free" },
  thumbnail: { type: String, default: "" },
  apiCalls: { type: String, default: "1K Calls" }
}, { timestamps: true });

const historySchema = new mongoose.Schema({
  apiId: { type: String, required: true },
  prompt: { type: String, default: "" },
  response: { type: mongoose.Schema.Types.Mixed },
  type: { type: String, default: "Text" }
}, { timestamps: true });

const settingSchema = new mongoose.Schema({
  key: { type: String, required: true, unique: true },
  value: { type: mongoose.Schema.Types.Mixed, required: true }
}, { timestamps: true });

const ticketSchema = new mongoose.Schema({
  id: { type: String, required: true, unique: true },
  name: { type: String, required: true },
  email: { type: String, required: true },
  category: { type: String, required: true },
  subject: { type: String, required: true },
  message: { type: String, required: true },
  status: { type: String, default: "OPEN" },
  messages: [{
    sender: { type: String, required: true },
    senderName: { type: String, required: true },
    text: { type: String, required: true },
    createdAt: { type: Date, default: Date.now }
  }]
}, { timestamps: true });

// --- BIDIRECTIONAL BACKUP & RESTORATION MANAGEMENT ---
const DB_PATH = path.join(process.cwd(), "db.json");
let backupInProgress = false;
let backupPending = false;

// We will define compiling models dynamically after schemas are fully prepared with hooks
// Attach automatic change triggers to preserve Mongoose writes immediately
const triggerBackup = async () => {
  // Backup is bypassed to rely completely on MongoDB persistence.
};

// Hooks are deactivated to prevent writing or overwriting database contents to/from db.json
// userSchema.post("save", triggerBackup);
// userSchema.post("remove", triggerBackup);

// apiSchema.post("save", triggerBackup);
// apiSchema.post("remove", triggerBackup);

// historySchema.post("save", triggerBackup);
// historySchema.post("remove", triggerBackup);

let User = mongoose.model("User", userSchema);
let Api = mongoose.model("Api", apiSchema);
let History = mongoose.model("History", historySchema);
let Setting = mongoose.model("Setting", settingSchema);
let Ticket = mongoose.model("Ticket", ticketSchema);

// --- FAIL-SAFE IN-MEMORY DATABASE FALLBACK SYSTEM ---
class MemoryCollection {
  constructor(name, defaultData = []) {
    this.name = name;
    this.filePath = path.join(process.cwd(), `db_${name.toLowerCase()}.json`);
    this.data = [];
    this.load(defaultData);
  }

  load(defaultData) {
    try {
      if (fs.existsSync(this.filePath)) {
        const raw = fs.readFileSync(this.filePath, 'utf8');
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed) && parsed.length > 0) {
          this.data = parsed.map(item => this.wrapDoc(item));
          console.log(`Loaded ${this.data.length} records for mock collection: ${this.name}`);
          return;
        }
      }
    } catch (err) {
      console.error(`Error loading database file for ${this.name}:`, err);
    }
    
    // Seed default data if file is empty or missing
    if (defaultData && defaultData.length > 0) {
      this.data = defaultData.map(item => this.wrapDoc(item));
      this.saveToFile();
      console.log(`Seeded and initialized ${this.data.length} default records for mock collection: ${this.name}`);
    }
  }

  saveToFile() {
    try {
      // Convert document instances back to plain objects for persistence
      const plain = this.data.map(doc => {
        const clean = { ...doc };
        delete clean.save;
        delete clean.markModified;
        return clean;
      });
      fs.writeFileSync(this.filePath, JSON.stringify(plain, null, 2), 'utf8');
    } catch (err) {
      console.error(`Error saving database file for ${this.name}:`, err);
    }
  }

  wrapDoc(item) {
    if (!item) return null;
    
    // Create a new object to avoid mutating frozen/existing objects in arrays
    const doc = {
      _id: item._id || Math.random().toString(36).substring(2, 11) + Date.now().toString(36),
      createdAt: item.createdAt || new Date().toISOString(),
      updatedAt: item.updatedAt || new Date().toISOString(),
      ...item
    };
    
    doc.save = async () => {
      doc.updatedAt = new Date().toISOString();
      const idx = this.data.findIndex(d => String(d._id) === String(doc._id));
      if (idx !== -1) {
        this.data[idx] = doc;
      } else {
        this.data.push(doc);
      }
      this.saveToFile();
      return doc;
    };

    doc.markModified = (field) => {
      // Polyfill to prevent "is not a function" errors
    };

    return doc;
  }

  async countDocuments(query = {}) {
    const list = await this.find(query);
    return list.length;
  }

  async find(query = {}) {
    let result = this.data.filter(item => {
      if (query._id) {
        if (typeof query._id === 'object') {
          if (query._id.$ne && String(item._id) === String(query._id.$ne)) return false;
          if (query._id.$in) {
            const list = query._id.$in.map(x => String(x));
            if (!list.includes(String(item._id))) return false;
          }
        } else {
          if (String(item._id) !== String(query._id)) return false;
        }
      }

      for (const key in query) {
        if (key === '_id') continue;
        
        const val = query[key];
        if (val && typeof val === 'object') {
          if (val.$ne && String(item[key]) === String(val.$ne)) return false;
          if (val.$in) {
            const list = val.$in.map(x => String(x));
            if (!list.includes(String(item[key]))) return false;
          }
        } else {
          if (String(item[key]) !== String(val)) return false;
        }
      }
      return true;
    });

    const chain = {
      sort: (sortCriteria) => {
        result.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
        return chain;
      },
      limit: (num) => {
        result = result.slice(0, num);
        return chain;
      },
      then: (resolve) => {
        const wrapped = result.map(d => this.wrapDoc(d));
        if (resolve) return Promise.resolve(wrapped).then(resolve);
        return Promise.resolve(wrapped);
      },
      catch: (reject) => {
        const wrapped = result.map(d => this.wrapDoc(d));
        return Promise.resolve(wrapped);
      }
    };

    // Support array inheritance / prototype chaining
    Object.setPrototypeOf(chain, Array.prototype);
    return chain;
  }

  async findOne(query = {}) {
    const list = await this.find(query);
    return list.length > 0 ? this.wrapDoc(list[0]) : null;
  }

  async findById(id) {
    if (!id) return null;
    const doc = this.data.find(item => String(item._id) === String(id));
    return doc ? this.wrapDoc(doc) : null;
  }

  async findByIdAndUpdate(id, update, options = {}) {
    if (!id) return null;
    const idx = this.data.findIndex(item => String(item._id) === String(id));
    if (idx === -1) return null;
    
    const existing = this.data[idx];
    const updated = {
      ...existing,
      ...update,
      _id: id,
      updatedAt: new Date().toISOString()
    };
    
    this.data[idx] = updated;
    this.saveToFile();
    return this.wrapDoc(updated);
  }

  async findByIdAndDelete(id) {
    if (!id) return null;
    const idx = this.data.findIndex(item => String(item._id) === String(id));
    if (idx === -1) return null;
    
    const removed = this.data.splice(idx, 1)[0];
    this.saveToFile();
    return this.wrapDoc(removed);
  }

  async insertMany(docs = []) {
    const created = docs.map(d => this.wrapDoc(d));
    this.data.push(...created);
    this.saveToFile();
    return created;
  }

  async create(doc) {
    const created = this.wrapDoc(doc);
    this.data.push(created);
    this.saveToFile();
    return created;
  }

  async deleteMany(query = {}) {
    if (query._id && query._id.$in) {
      const ids = query._id.$in.map(x => String(x));
      this.data = this.data.filter(item => !ids.includes(String(item._id)));
    } else {
      this.data = [];
    }
    this.saveToFile();
    return { acknowledged: true, deletedCount: this.data.length };
  }
}

function createMockModel(name, collectionInstance) {
  function ModelClass(data = {}) {
    return collectionInstance.wrapDoc(data);
  }
  ModelClass.countDocuments = (q) => collectionInstance.countDocuments(q);
  ModelClass.find = (q) => collectionInstance.find(q);
  ModelClass.findOne = (q) => collectionInstance.findOne(q);
  ModelClass.findById = (id) => collectionInstance.findById(id);
  ModelClass.findByIdAndUpdate = (id, up, opts) => collectionInstance.findByIdAndUpdate(id, up, opts);
  ModelClass.findByIdAndDelete = (id) => collectionInstance.findByIdAndDelete(id);
  ModelClass.insertMany = (docs) => collectionInstance.insertMany(docs);
  ModelClass.create = (doc) => collectionInstance.create(doc);
  ModelClass.deleteMany = (query) => collectionInstance.deleteMany(query);
  return ModelClass;
}

async function saveDbBackup() {
  // Bypassed: Writing database backups to db.json is deactivated.
}

async function restoreDbFromBackup() {
  // Bypassed: Restoring database contents from db.json is deactivated.
  // The database will now persist directly in MongoDB without getting wiped or overwritten on startup.
  return false;
}

// Connection and Seeding Manager
async function initializeDatabase() {
  let connected = false;
  const uri = process.env.MONGODB_URI || "mongodb+srv://itsnexverra_db_user:C5NpAE6r2cuyQmEm@cluster0.kgjavvq.mongodb.net/?appName=Cluster0";

  console.log(`[Database] Attempting connection to MongoDB Atlas database cluster: ${uri.replace(/:([^@]+)@/, ":*****@")}`);

  // Disable Mongoose query buffering so that queries return errors immediately during disconnection rather than hanging
  mongoose.set('bufferCommands', false);

  try {
    // Attempt connecting to the external MongoDB Atlas instance with a low timeout to prevent startup 503 gateway timeouts
    await mongoose.connect(uri, {
      serverSelectionTimeoutMS: 4000, // 4 seconds total for choosing/connecting to Atlas
      connectTimeoutMS: 5000,         // 5 seconds connection handshake
    });
    console.log("[Database] Connected successfully to MongoDB Atlas!");
    connected = true;
  } catch (err) {
    console.error(`[Database] Connection to Atlas failed: ${err.message}. Enabling seamless fully-functional in-memory JSON fallback database...`);
  }

  if (!connected) {
    // Fallback: Bind to MemoryCollection local JSON databases immediately
    console.log("⚠️ ALL MongoDB Atlas options failed. Seamlessly activating Bulletproof in-memory Fallback Database System...");

    const adminPassword = await bcrypt.hash("admin123", 10);
    const userPassword = await bcrypt.hash("user123", 10);
    const targetedAdminPassword = await bcrypt.hash("123@Rahul", 10);

    const initialUsers = [
      { email: "admin@demo.com", password: adminPassword, role: "admin", name: "Demo Admin", coins: 200, subscribedApis: [], subscribedPlan: "" },
      { email: "user@demo.com", password: userPassword, role: "user", name: "Demo User", coins: 200, subscribedApis: [], subscribedPlan: "" },
      { email: "itsdevelopersarmy@gmail.com", password: targetedAdminPassword, role: "admin", name: "Its Developers Army Admin", coins: 99999, subscribedApis: [], subscribedPlan: "" }
    ];

    const initialApis = [
      {
        name: "GPT-4o Vision",
        key: "sk-demo-123",
        description: "Advanced multimodal model capable of processing text and images with high precision.",
        endpoint: "",
        coinsPerCall: 15,
        subscriptionPrice: 1500,
        type: "API",
        category: "Text",
        curlTemplate: JSON.stringify({ model: "gpt-4o", messages: [{ role: "user", content: "{{PROMPT}}" }] }, null, 2),
        published: true,
        planBatch: "free",
        apiCalls: "45.2K Calls"
      },
      {
        name: "Stable Diffusion XL",
        key: "sd-demo-456",
        description: "State-of-the-art image generation model for high-fidelity artistic visuals.",
        endpoint: "",
        coinsPerCall: 25,
        subscriptionPrice: 2500,
        type: "Modal",
        category: "Image",
        curlTemplate: JSON.stringify({ prompt: "{{PROMPT}}", negative_prompt: "low quality", steps: 30 }, null, 2),
        published: true,
        planBatch: "pro",
        apiCalls: "112.5K Calls"
      },
      {
        name: "Claude 3.5 Sonnet",
        key: "ant-demo-789",
        description: "Highly intelligent model optimized for complex reasoning and creative writing.",
        endpoint: "",
        coinsPerCall: 10,
        subscriptionPrice: 1000,
        type: "API",
        category: "Text",
        curlTemplate: JSON.stringify({ prompt: "{{PROMPT}}", max_tokens: 1000 }, null, 2),
        published: true,
        planBatch: "creator",
        apiCalls: "84.1K Calls"
      },
      {
        name: "Whisper V3",
        key: "wh-demo-000",
        description: "Robust speech-to-text model with multi-language support and high accuracy.",
        endpoint: "",
        coinsPerCall: 5,
        subscriptionPrice: 500,
        type: "API",
        category: "Audio",
        curlTemplate: JSON.stringify({ audio_url: "{{PROMPT}}", task: "transcribe" }, null, 2),
        published: true,
        planBatch: "creator",
        apiCalls: "23.4K Calls"
      },
      {
        name: "Sora Video Gen",
        key: "so-demo-111",
        description: "Experimental video generation model creating realistic cinematic sequences.",
        endpoint: "",
        coinsPerCall: 100,
        subscriptionPrice: 10000,
        type: "Modal",
        category: "Video",
        curlTemplate: JSON.stringify({ prompt: "{{PROMPT}}", duration: "5s" }, null, 2),
        published: true,
        planBatch: "studio",
        apiCalls: "8.9K Calls"
      },
      {
        name: "DALL-E 3",
        key: "dl-demo-222",
        description: "OpenAI's premier image generation model with deep prompt understanding.",
        endpoint: "",
        coinsPerCall: 30,
        subscriptionPrice: 3000,
        type: "API",
        category: "Image",
        curlTemplate: JSON.stringify({ prompt: "{{PROMPT}}", quality: "hd" }, null, 2),
        published: true,
        planBatch: "pro",
        apiCalls: "94.6K Calls"
      },
      {
        name: "Gemini 1.5 Pro",
        key: "gem-demo-333",
        description: "Google's most capable model with a massive context window for deep analysis.",
        endpoint: "",
        coinsPerCall: 20,
        subscriptionPrice: 2000,
        type: "API",
        category: "Text",
        curlTemplate: JSON.stringify({ contents: [{ parts: [{ text: "{{PROMPT}}" }] }] }, null, 2),
        published: true,
        planBatch: "studio",
        apiCalls: "62.1K Calls"
      },
      {
        name: "Midjourney V6",
        key: "mj-demo-444",
        description: "Professional grade image generation with unparalleled artistic style.",
        endpoint: "",
        coinsPerCall: 40,
        subscriptionPrice: 4000,
        type: "Modal",
        category: "Image",
        curlTemplate: JSON.stringify({ prompt: "{{PROMPT}} --v 6.0" }, null, 2),
        published: true,
        planBatch: "studio",
        apiCalls: "128.3K Calls"
      }
    ];

    const initialSettings = [
      {
        key: "theme",
        value: {
          mode: "light",
          accentColor: "#06b6d4",
          accentHover: "#0891b2",
          accentLight: "#ecfeff",
          accentDark: "#164e63",
          accentBorder: "#cffafe"
        }
      }
    ];

    const usersCol = new MemoryCollection("User", initialUsers);
    const apisCol = new MemoryCollection("Api", initialApis);
    const historyCol = new MemoryCollection("History", []);
    const settingsCol = new MemoryCollection("Setting", initialSettings);
    const ticketsCol = new MemoryCollection("Ticket", []);

    User = createMockModel("User", usersCol);
    Api = createMockModel("Api", apisCol);
    History = createMockModel("History", historyCol);
    Setting = createMockModel("Setting", settingsCol);
    Ticket = createMockModel("Ticket", ticketsCol);

    console.log("✅ Models loaded to Memory fallback. Server is 100% online and responding immediately!");
    return;
  }

  // Restore existing backup first if present
  let restored = false;
  try {
    restored = await restoreDbFromBackup();
  } catch (err) {
    console.error("Backup restore failed:", err);
  }

  // If connected successfully to Mongo Atlas, run seeding
  try {
    const userCount = await User.countDocuments();
    if (userCount === 0) {
      console.log("Seeding demo users into Atlas...");
      const adminPassword = await bcrypt.hash("admin123", 10);
      const userPassword = await bcrypt.hash("user123", 10);
      
      await User.insertMany([
        { email: "admin@demo.com", password: adminPassword, role: "admin", name: "Demo Admin" },
        { email: "user@demo.com", password: userPassword, role: "user", name: "Demo User" }
      ]);
      console.log("Demo users seeded successfully in Atlas!");
    } else {
      console.log(`Found ${userCount} existing users in Atlas.`);
    }

    // Force-seed the special admin account
    const targetedAdminEmail = "itsdevelopersarmy@gmail.com";
    const targetedAdminPassword = await bcrypt.hash("123@Rahul", 10);
    const existingTargetAdmin = await User.findOne({ email: targetedAdminEmail });
    if (!existingTargetAdmin) {
      console.log("Creating special admin user in Atlas...");
      await User.create({
        email: targetedAdminEmail,
        password: targetedAdminPassword,
        role: "admin",
        name: "Its Developers Army Admin"
      });
    } else {
      console.log("Updating special admin user credentials in Atlas...");
      existingTargetAdmin.role = "admin";
      existingTargetAdmin.password = targetedAdminPassword;
      await existingTargetAdmin.save();
    }

    const apiCount = await Api.countDocuments();
    if (apiCount === 0) {
      console.log("Seeding dummy API data into Atlas...");
      const dummyApis = [
        {
          name: "GPT-4o Vision",
          key: "sk-demo-123",
          description: "Advanced multimodal model capable of processing text and images with high precision.",
          endpoint: "",
          coinsPerCall: 15,
          type: "API",
          category: "Text",
          curlTemplate: JSON.stringify({ model: "gpt-4o", messages: [{ role: "user", content: "{{PROMPT}}" }] }, null, 2),
          published: true,
          planBatch: "free",
          apiCalls: "45.2K Calls"
        },
        {
          name: "Stable Diffusion XL",
          key: "sd-demo-456",
          description: "State-of-the-art image generation model for high-fidelity artistic visuals.",
          endpoint: "",
          coinsPerCall: 25,
          type: "Modal",
          category: "Image",
          curlTemplate: JSON.stringify({ prompt: "{{PROMPT}}", negative_prompt: "low quality", steps: 30 }, null, 2),
          published: true,
          planBatch: "pro",
          apiCalls: "112.5K Calls"
        },
        {
          name: "Claude 3.5 Sonnet",
          key: "ant-demo-789",
          description: "Highly intelligent model optimized for complex reasoning and creative writing.",
          endpoint: "",
          coinsPerCall: 10,
          type: "API",
          category: "Text",
          curlTemplate: JSON.stringify({ prompt: "{{PROMPT}}", max_tokens: 1000 }, null, 2),
          published: true,
          planBatch: "creator",
          apiCalls: "84.1K Calls"
        },
        {
          name: "Whisper V3",
          key: "wh-demo-000",
          description: "Robust speech-to-text model with multi-language support and high accuracy.",
          endpoint: "",
          coinsPerCall: 5,
          type: "API",
          category: "Audio",
          curlTemplate: JSON.stringify({ audio_url: "{{PROMPT}}", task: "transcribe" }, null, 2),
          published: true,
          planBatch: "creator",
          apiCalls: "23.4K Calls"
        },
        {
          name: "Sora Video Gen",
          key: "so-demo-111",
          description: "Experimental video generation model creating realistic cinematic sequences.",
          endpoint: "",
          coinsPerCall: 100,
          type: "Modal",
          category: "Video",
          curlTemplate: JSON.stringify({ prompt: "{{PROMPT}}", duration: "5s" }, null, 2),
          published: true,
          planBatch: "studio",
          apiCalls: "8.9K Calls"
        },
        {
          name: "DALL-E 3",
          key: "dl-demo-222",
          description: "OpenAI's premier image generation model with deep prompt understanding.",
          endpoint: "",
          coinsPerCall: 30,
          type: "API",
          category: "Image",
          curlTemplate: JSON.stringify({ prompt: "{{PROMPT}}", quality: "hd" }, null, 2),
          published: true,
          planBatch: "pro",
          apiCalls: "94.6K Calls"
        },
        {
          name: "Gemini 1.5 Pro",
          key: "gem-demo-333",
          description: "Google's most capable model with a massive context window for deep analysis.",
          endpoint: "",
          coinsPerCall: 20,
          type: "API",
          category: "Text",
          contents: [{ parts: [{ text: "{{PROMPT}}" }] }],
          published: true,
          planBatch: "studio",
          apiCalls: "62.1K Calls"
        },
        {
          name: "Midjourney V6",
          key: "mj-demo-444",
          description: "Professional grade image generation with unparalleled artistic style.",
          endpoint: "",
          coinsPerCall: 40,
          type: "Modal",
          category: "Image",
          curlTemplate: JSON.stringify({ prompt: "{{PROMPT}} --v 6.0" }, null, 2),
          published: true,
          planBatch: "studio",
          apiCalls: "128.3K Calls"
        }
      ];
      await Api.insertMany(dummyApis);
      console.log("Database seeded with sample model endpoints successfully in Atlas!");
    } else {
      console.log(`Found ${apiCount} existing APIs in Atlas.`);
    }

    // Seed default theme settings if they do not exist
    const themeSetting = await Setting.findOne({ key: "theme" });
    if (!themeSetting) {
      console.log("Seeding default theme settings into Atlas...");
      await Setting.create({
        key: "theme",
        value: {
          mode: "light",
          accentColor: "#06b6d4",
          accentHover: "#0891b2",
          accentLight: "#ecfeff",
          accentDark: "#164e63",
          accentBorder: "#cffafe"
        }
      });
      console.log("Default theme settings seeded successfully in Atlas!");
    } else {
      console.log("Existing theme settings detected in Atlas:", themeSetting.value);
    }

    // Capture the initial snapshot to the file storage if not restored
    if (!restored) {
      await saveDbBackup();
    }
  } catch (err) {
    console.error("Database seeding encountered an issue in Atlas:", err);
  }
}

// Ingest DB initialization asynchronously
initializeDatabase();

// Auth Middleware
const authenticateToken = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) return res.status(401).json({ error: "Access denied" });

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) return res.status(403).json({ error: "Invalid token" });
    req.user = user;
    next();
  });
};

const isAdmin = (req, res, next) => {
  if (req.user.role !== 'admin') {
    return res.status(403).json({ error: "Admin access required" });
  }
  next();
};

// Auth Routes
app.post("/api/auth/signup", async (req, res) => {
  try {
    const { email, password, name } = req.body;
    const existingUser = await User.findOne({ email });
    if (existingUser) return res.status(400).json({ error: "User already exists" });

    const hashedPassword = await bcrypt.hash(password, 10);
    const user = new User({ email, password: hashedPassword, name });
    await user.save();

    const token = jwt.sign({ id: user._id, role: user.role, name: user.name }, JWT_SECRET);
    res.json({ token, user: { email: user.email, name: user.name, role: user.role, coins: user.coins, subscribedApis: user.subscribedApis || [], subscribedPlan: user.subscribedPlan || "" } });
  } catch (error) {
    res.status(500).json({ error: "Signup failed" });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    const user = await User.findOne({ email });
    if (!user) return res.status(400).json({ error: "User not found" });

    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) return res.status(400).json({ error: "Invalid password" });

    const token = jwt.sign({ id: user._id, role: user.role, name: user.name }, JWT_SECRET);
    res.json({ token, user: { email: user.email, name: user.name, role: user.role, coins: user.coins, subscribedApis: user.subscribedApis || [], subscribedPlan: user.subscribedPlan || "" } });
  } catch (error) {
    res.status(500).json({ error: "Login failed" });
  }
});

// Profile & Coin Synchronization Routes
app.get("/api/auth/me", authenticateToken, async (req, res) => {
  try {
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: "User not found" });
    
    res.json({ email: user.email, name: user.name, role: user.role, coins: user.coins, subscribedApis: user.subscribedApis || [], subscribedPlan: user.subscribedPlan || "" });
  } catch (error) {
    res.status(500).json({ error: "Failed to get user profile" });
  }
});

app.post("/api/user/deduct", authenticateToken, async (req, res) => {
  try {
    const { amount } = req.body;
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: "User not found" });

    const deductAmount = Number(amount) || 5;
    user.coins = Math.max(0, user.coins - deductAmount);
    await user.save();
    await saveDbBackup();

    res.json({ success: true, coins: user.coins, message: `${deductAmount} coins deducted successfully.` });
  } catch (error) {
    console.error("Deduct coins failed:", error);
    res.status(500).json({ error: "Failed to deduct coins" });
  }
});

app.post("/api/payment/confirm", authenticateToken, async (req, res) => {
  try {
    const { apiId, planId } = req.body;
    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: "User not found" });

    if (planId) {
      user.subscribedPlan = planId;
      if (planId === "creator") {
        user.coins += 8000;
      } else if (planId === "pro") {
        user.coins += 12000;
      } else if (planId === "studio") {
        user.coins += 20000;
      }
    }

    if (apiId) {
      // Ensure array exists
      if (!user.subscribedApis) {
        user.subscribedApis = [];
      }
      
      // Add to subscription list if not already present
      if (!user.subscribedApis.includes(apiId)) {
        user.subscribedApis.push(apiId);
      }

      // Only credit additional 500 coins if they subscribe strictly to the api without upgrading plan
      if (!planId) {
        user.coins += 500;
      }
    }
    
    await user.save();
    await saveDbBackup();

    res.json({
      success: true,
      coins: user.coins,
      subscribedApis: user.subscribedApis || [],
      subscribedPlan: user.subscribedPlan || "",
      message: planId ? `Successfully subscribed to ${planId.toUpperCase()} Plan!` : "Subscription completed and model unlocked successfully."
    });
  } catch (error) {
    console.error("Payment validation failed:", error);
    res.status(500).json({ error: "Failed to validate payment" });
  }
});

// Admin User Management Routes
app.get("/api/admin/users", authenticateToken, isAdmin, async (req, res) => {
  try {
    const users = await User.find({}, { password: 0 });
    res.json(users);
  } catch (error) {
    console.error("Failed to fetch users list for admin:", error);
    res.status(500).json({ error: "Failed to fetch users list" });
  }
});

app.put("/api/admin/users/:id/subscription", authenticateToken, isAdmin, async (req, res) => {
  try {
    const { subscribedApis, coins, role, name, email, password, subscribedPlan } = req.body;
    const user = await User.findById(req.params.id);
    if (!user) return res.status(404).json({ error: "User not found" });

    if (subscribedApis !== undefined) {
      user.subscribedApis = subscribedApis;
    }
    if (subscribedPlan !== undefined) {
      user.subscribedPlan = subscribedPlan;
    }
    if (coins !== undefined) {
      user.coins = Number(coins) || 0;
    }
    if (role !== undefined) {
      user.role = role;
    }
    if (name !== undefined) {
      user.name = name;
    }
    if (email !== undefined) {
      const existing = await User.findOne({ email, _id: { $ne: req.params.id } });
      if (existing) {
        return res.status(400).json({ error: "Email already taken by another user." });
      }
      user.email = email;
    }
    if (password) {
      user.password = await bcrypt.hash(password, 10);
    }

    await user.save();
    await saveDbBackup();

    res.json({
      success: true,
      message: "User settings updated successfully.",
      user: {
        _id: user._id,
        name: user.name,
        email: user.email,
        role: user.role,
        coins: user.coins,
        subscribedApis: user.subscribedApis || [],
        subscribedPlan: user.subscribedPlan || ""
      }
    });
  } catch (error) {
    console.error("Failed to update user setting:", error);
    res.status(500).json({ error: "Failed to update user" });
  }
});

app.delete("/api/admin/users/:id", authenticateToken, isAdmin, async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    if (!user) return res.status(404).json({ error: "User not found" });

    if (String(user._id) === String(req.user.id)) {
      return res.status(400).json({ error: "Cannot delete your own admin account!" });
    }

    await User.findByIdAndDelete(req.params.id);
    await saveDbBackup();

    res.json({
      success: true,
      message: "User deleted successfully."
    });
  } catch (error) {
    console.error("Failed to delete user:", error);
    res.status(500).json({ error: "Failed to delete user" });
  }
});

// API Routes
app.get("/api/theme", async (req, res) => {
  try {
    const themeSetting = await Setting.findOne({ key: "theme" });
    if (!themeSetting) {
      return res.json({
        mode: "light",
        accentColor: "#06b6d4",
        accentHover: "#0891b2",
        accentLight: "#ecfeff",
        accentDark: "#164e63",
        accentBorder: "#cffafe"
      });
    }
    res.json(themeSetting.value);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch theme" });
  }
});

app.put("/api/theme", authenticateToken, isAdmin, async (req, res) => {
  try {
    const { mode, accentColor, accentHover, accentLight, accentDark, accentBorder } = req.body;
    
    const themeValue = {
      mode: mode || "light",
      accentColor: accentColor || "#06b6d4",
      accentHover: accentHover || "#0891b2",
      accentLight: accentLight || "#ecfeff",
      accentDark: accentDark || "#164e63",
      accentBorder: accentBorder || "#cffafe"
    };

    let themeSetting = await Setting.findOne({ key: "theme" });
    if (!themeSetting) {
      themeSetting = new Setting({ key: "theme", value: themeValue });
    } else {
      themeSetting.value = { ...themeSetting.value, ...themeValue };
    }
    themeSetting.markModified('value');
    await themeSetting.save();
    console.log("Global theme settings updated:", themeSetting.value);
    res.json(themeSetting.value);
  } catch (error) {
    console.error("Failed to update theme settings:", error);
    res.status(500).json({ error: "Failed to update theme settings" });
  }
});

app.get("/api/models", authenticateToken, isAdmin, async (req, res) => {
  try {
    const apis = await Api.find().sort({ createdAt: -1 });
    res.json(apis);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch APIs" });
  }
});

app.get("/api/models/published", async (req, res) => {
  try {
    const apis = await Api.find({ published: true }).sort({ createdAt: -1 });
    res.json(apis);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch published APIs" });
  }
});

app.post("/api/models", authenticateToken, isAdmin, async (req, res) => {
  try {
    console.log("Creating model with body:", JSON.stringify(req.body, null, 2));
    const api = new Api(req.body);
    await api.save();
    await saveDbBackup();
    res.json(api);
  } catch (error) {
    console.error("Create error:", error);
    res.status(500).json({ error: "Failed to create API" });
  }
});

app.put("/api/models/:id", authenticateToken, isAdmin, async (req, res) => {
  try {
    console.log(`Updating model ${req.params.id} with body:`, JSON.stringify(req.body, null, 2));
    const { _id, createdAt, updatedAt, __v, ...updateData } = req.body;
    const api = await Api.findByIdAndUpdate(
      req.params.id, 
      updateData, 
      { new: true, runValidators: true }
    );
    console.log("Updated model result:", api);
    await saveDbBackup();
    res.json(api);
  } catch (error) {
    console.error("Update error:", error);
    res.status(500).json({ error: "Failed to update API" });
  }
});

app.delete("/api/models/:id", authenticateToken, isAdmin, async (req, res) => {
  try {
    await Api.findByIdAndDelete(req.params.id);
    await saveDbBackup();
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: "Failed to delete API" });
  }
});

app.post("/api/models/bulk-delete", authenticateToken, isAdmin, async (req, res) => {
  try {
    const { ids } = req.body;
    if (!ids || !Array.isArray(ids)) {
      return res.status(400).json({ error: "Invalid IDs provided" });
    }
    await Api.deleteMany({ _id: { $in: ids } });
    await saveDbBackup();
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: "Failed to bulk delete APIs" });
  }
});

// Test API Execution
app.post("/api/test/:id", authenticateToken, async (req, res) => {
  try {
    const api = await Api.findById(req.params.id);
    if (!api) return res.status(404).json({ error: "API not found" });

    const user = await User.findById(req.user.id);
    if (!user) return res.status(404).json({ error: "User not found" });

    const plan = user.subscribedPlan || "";
    let isSubscribed = user.role === "admin";
    if (!isSubscribed) {
      if (api.planBatch === "free" || !api.planBatch) {
        isSubscribed = true;
      } else if (plan) {
        const activePlans = plan.split(',').map(p => p.trim().toLowerCase());
        isSubscribed = activePlans.includes((api.planBatch || "").toLowerCase());
      } else {
        isSubscribed = user.subscribedApis && user.subscribedApis.includes(String(api._id));
      }
    }

    if (!isSubscribed && user.coins < api.coinsPerCall) {
      return res.status(403).json({ error: "Insufficient coins. Please contact support to top up." });
    }

    const { prompt, aspectRatio } = req.body;
    let result;

    if (api.endpoint && api.endpoint.startsWith("http")) {
      try {
        console.log(`Executing real API call to: ${api.endpoint}`);
        
        let payload = {};
        if (api.curlTemplate) {
          try {
            // Remove any leading/trailing whitespace and potentially "curl" prefix if pasted as raw command
            let cleanedTemplate = api.curlTemplate.trim();
            if (cleanedTemplate.toLowerCase().startsWith("curl")) {
              console.warn("Detected 'curl' prefix in template, this system expects the JSON body or a structured template.");
            }

            let templateStr = cleanedTemplate
              .replace(/{{PROMPT}}/g, prompt)
              .replace(/{{KEY}}/g, api.key || "")
              .replace(/{{RATIO}}/g, aspectRatio || "1:1");
            
            payload = JSON.parse(templateStr);
          } catch (e) {
            console.warn("JSON parsing of curlTemplate failed, using fallback payload:", e.message);
            payload = { prompt, key: api.key };
          }
        } else {
          payload = { prompt, key: api.key };
        }

        const headers = {
          'Content-Type': 'application/json'
        };

        const rapidApiKey = process.env.RAPID_API_KEY || process.env.RAPIDAPI_KEY || api.key;
        
        if (api.endpoint && api.endpoint.includes("rapidapi.com")) {
          try {
            headers['x-rapidapi-host'] = new URL(api.endpoint).hostname;
          } catch (e) {
            console.warn("Could not extract hostname for rapidapi endpoint:", e.message);
          }
          if (rapidApiKey) {
            headers['x-rapidapi-key'] = rapidApiKey;
          }
        } else if (api.key) {
          if (api.endpoint.includes("anthropic") || api.endpoint.includes("claude")) {
            headers['x-api-key'] = api.key;
            headers['anthropic-version'] = '2023-06-01';
          } else if (api.endpoint.includes("replicate.com")) {
            headers['Authorization'] = `Token ${api.key}`;
          } else {
            // High compatibility mode: send key in common headers used by various providers
            headers['Authorization'] = `Bearer ${api.key}`;
            headers['x-api-key'] = api.key;
            headers['api-key'] = api.key;
            headers['apikey'] = api.key;
          }
        }

        const response = await axios.post(api.endpoint, payload, {
          headers,
          timeout: 60000 // Increased to 60s for slow image gen
        });

        let data = response.data;
        
        // Helper to deeply search for all URLs and Base64 data in the response object
        const findAllMediaUrls = (obj, collected = [], parentKey = "") => {
          if (!obj) return collected;
          
          if (typeof obj === "string") {
            const trimmed = obj.trim();
            // Case 1: Already a valid URL or Data URI
            if (trimmed.startsWith("http") || trimmed.startsWith("data:")) {
              try { 
                if (trimmed.startsWith("data:")) {
                  if (!collected.includes(trimmed)) collected.push(trimmed);
                  return collected;
                }
                new URL(trimmed); 
                const ignoreList = ["schema.org", "w3.org", "google.com", "facebook.com", "twitter.com"];
                if (ignoreList.some(domain => trimmed.includes(domain))) return collected;
                
                if (!collected.includes(trimmed)) collected.push(trimmed);
                return collected; 
              } catch (e) { 
                return collected; 
              }
            }
            // Case 2: Raw Base64 string from common keys like b64_json
            if (parentKey === "b64_json" && trimmed.length > 100) {
              const dataUri = `data:image/png;base64,${trimmed}`;
              if (!collected.includes(dataUri)) collected.push(dataUri);
              return collected;
            }
          }
          
          if (Array.isArray(obj)) {
            for (const item of obj) {
              findAllMediaUrls(item, collected, parentKey);
            }
          } else if (typeof obj === "object") {
            const priorityKeys = ["images", "outputs", "results", "data", "generations", "artifacts"];
            for (const key of priorityKeys) {
              if (obj[key]) {
                findAllMediaUrls(obj[key], collected, key);
              }
            }
            
            const commonKeys = ["url", "image", "img", "output", "link", "src", "uri", "audio", "video", "audio_url", "video_url", "url_private", "b64_json"];
            for (const key of commonKeys) {
              if (obj[key]) {
                findAllMediaUrls(obj[key], collected, key);
              }
            }
            
            for (const key in obj) {
              if (!priorityKeys.includes(key) && !commonKeys.includes(key)) {
                findAllMediaUrls(obj[key], collected, key);
              }
            }
          }
          return collected;
        };

        console.log("Raw API Response Data:", JSON.stringify(data, null, 2));
        let mediaUrls = findAllMediaUrls(data);
        console.log("Detected Media URLs:", mediaUrls);

        // Polling logic for asynchronous APIs
        const genId = data.generationId || data.id || (data.sdGenerationJob && data.sdGenerationJob.generationId) || (data.input && data.input.id);
        if (mediaUrls.length === 0 && genId && api.endpoint) {
          console.log(`Detected asynchronous job (${genId}). Attempting to poll for results...`);
          
          let pollUrl = null;
          if (data.status_url) {
            pollUrl = data.status_url;
          } else if (api.endpoint.includes("leonardo.ai")) {
            pollUrl = `https://cloud.leonardo.ai/api/rest/v1/generations/${genId}`;
          } else if (api.endpoint.includes("replicate.com")) {
            pollUrl = `https://api.replicate.com/v1/predictions/${genId}`;
          } else if (api.endpoint.includes("stablehorde.net") || data.kudos) {
            // Support for AI Horde (Stable Horde)
            const baseUrl = api.endpoint.split('/v2/')[0];
            pollUrl = `${baseUrl}/v2/generate/status/${genId}`;
          } else if (api.endpoint.includes("/async") || api.endpoint.includes("/jobs")) {
            // Heuristic for other async/job based endpoints
            if (api.endpoint.includes("/v1/")) {
              const baseUrl = api.endpoint.split('/v1/')[0];
              pollUrl = `${baseUrl}/v1/status/${genId}`;
            }
          }

          if (pollUrl) {
            console.log(`Polling URL resolved to: ${pollUrl}`);
            for (let i = 0; i < 12; i++) { // Increased to 12 attempts (60s)
              await new Promise(resolve => setTimeout(resolve, 5000));
              try {
                const pollResponse = await axios.get(pollUrl, { headers });
                data = pollResponse.data;
                mediaUrls = findAllMediaUrls(data);
                
                // AI Horde specifically wraps results in 'generations' array within data
                if (mediaUrls.length > 0) {
                  console.log("Media found during polling!");
                  break;
                }

                // Check for completion status in common API formats
                const status = (data.status || data.state || "").toLowerCase();
                if (status === "failed" || status === "canceled" || status === "error") {
                  console.error(`Job terminated with status: ${status}`);
                  break;
                }
                
                console.log(`Polling attempt ${i + 1}: Status is ${status || "processing"}...`);
              } catch (pollErr) {
                console.error("Polling attempt failed:", pollErr.message);
              }
            }
          } else {
            console.warn("Could not resolve a status/polling URL for this asynchronous request.");
          }
        }

        if (api.category === "Image" || api.type === "Modal" || (api.name && api.name.toLowerCase().includes("image"))) {
          if (mediaUrls.length > 0) {
            result = { type: "image", url: mediaUrls[0], urls: mediaUrls };
          } else {
            const seed = Math.floor(Math.random() * 1000000);
            const imageUrl = `https://image.pollinations.ai/prompt/${encodeURIComponent(prompt || "beautiful artwork")}?width=1024&height=1024&nologo=true&seed=${seed}`;
            result = { type: "image", url: imageUrl, urls: [imageUrl] };
          }
        } else if (api.category === "Audio") {
          if (mediaUrls.length > 0) {
            result = { type: "audio", url: mediaUrls[0], urls: mediaUrls };
          } else {
            result = { type: "text", content: `API returned success but no audio URL was found after polling. \n\nFinal Response: ${JSON.stringify(data, null, 2)}` };
          }
        } else if (api.category === "Video") {
          if (mediaUrls.length > 0) {
            result = { type: "video", url: mediaUrls[0], urls: mediaUrls };
          } else {
            result = { type: "text", content: `API returned success but no video URL was found after polling. \n\nFinal Response: ${JSON.stringify(data, null, 2)}` };
          }
        } else {
          result = { 
            type: "text", 
            content: typeof data === 'string' ? data : JSON.stringify(data, null, 2) 
          };
        }
      } catch (apiError) {
        console.error("External API call failed:", apiError.message);
        if (api.category === "Image" || api.type === "Modal" || (api.name && api.name.toLowerCase().includes("image"))) {
          const seed = Math.floor(Math.random() * 1000000);
          const imageUrl = `https://image.pollinations.ai/prompt/${encodeURIComponent(prompt || "beautiful artwork")}?width=1024&height=1024&nologo=true&seed=${seed}`;
          result = { type: "image", url: imageUrl, urls: [imageUrl] };
        } else {
          const errorDetail = apiError.response?.data ? JSON.stringify(apiError.response.data) : apiError.message;
          result = { 
            type: "text", 
            content: `API Error: ${errorDetail}. Please check your endpoint, key, and payload template.` 
          };
        }
      }
    } else {
      if (api.category === "Image" || api.type === "Modal" || (api.name && api.name.toLowerCase().includes("image"))) {
        const seed = Math.floor(Math.random() * 1000000);
        const imageUrl = `https://image.pollinations.ai/prompt/${encodeURIComponent(prompt || "beautiful artwork")}?width=1024&height=1024&nologo=true&seed=${seed}`;
        result = { type: "image", url: imageUrl, urls: [imageUrl] };
      } else {
        result = {
          type: "text",
          content: `Configuration Error: No valid API endpoint configured for "${api.name}". Please go to the Admin Dashboard and set a valid URL.`
        };
      }
    }

    // Save to history
    const history = new History({
      apiId: api._id,
      prompt,
      response: result,
      type: api.category
    });
    await history.save();

    // Deduct coins only if generation was successful (not an error message) and noDeduct is not set to true
    let finalCoins = user.coins;
    if (req.query.noDeduct !== "true" && !isSubscribed) {
      if (result.type !== "text" || !result.content?.startsWith("API Error:")) {
        user.coins = Math.max(0, user.coins - api.coinsPerCall);
        await user.save();
        await saveDbBackup();
        finalCoins = user.coins;
      }
    }

    res.json({ ...result, updatedCoins: finalCoins, historyId: history._id });
  } catch (error) {
    console.error("Execution failed:", error);
    res.status(500).json({ error: "Execution failed" });
  }
});

app.get("/api/history/user/all", authenticateToken, async (req, res) => {
  try {
    const history = await History.find({}).sort({ createdAt: -1 }).limit(50);
    res.json(history);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch history" });
  }
});

app.delete("/api/history/:id", authenticateToken, async (req, res) => {
  try {
    await History.findByIdAndDelete(req.params.id);
    res.json({ success: true, message: "History entry deleted" });
  } catch (error) {
    res.status(500).json({ error: "Failed to delete history item" });
  }
});

app.get("/api/history/:apiId", authenticateToken, async (req, res) => {
  try {
    const history = await History.find({ apiId: req.params.apiId }).sort({ createdAt: -1 }).limit(30);
    res.json(history);
  } catch (error) {
    res.status(500).json({ error: "Failed to fetch history" });
  }
});

// QR Proxy Endpoint
app.get("/api/payment/qr-proxy", async (req, res) => {
  try {
    const { vpa, amount, name = "API Tester" } = req.query;
    
    console.log(`Generating local UPI QR request for: ${vpa}, Amount: ${amount}`);

    if (!vpa) {
      return res.status(400).json({ error: "VPA address is required" });
    }

    // Construct the standard UPI payment address URL
    // Format: upi://pay?pa=address@bank&pn=PayeeName&am=Amount&cu=INR
    let formattedAmount = amount;
    if (amount) {
      const parsedAmount = parseFloat(amount);
      if (!isNaN(parsedAmount)) {
        formattedAmount = parsedAmount.toFixed(2);
      }
    }

    const upiUrl = `upi://pay?pa=${vpa}&pn=${encodeURIComponent(name)}&am=${formattedAmount}&cu=INR`;
    
    // Generate the QR code as a PNG buffer
    const qrBuffer = await QRCode.toBuffer(upiUrl, {
      type: 'png',
      width: 512,
      margin: 2,
      color: {
        dark: '#0a1b3d', // Matching dark blue theme
        light: '#ffffff'
      }
    });

    res.set('Content-Type', 'image/png');
    res.send(qrBuffer);
  } catch (error) {
    console.error("QR Generation failed:", error.message);
    res.status(500).json({ error: "Failed to generate QR code" });
  }
});

// --- SUPPORT TICKETS & CHAT API ENDPOINTS ---
app.post("/api/tickets", async (req, res) => {
  try {
    const { id, name, email, category, subject, message } = req.body;
    if (!id || !name || !email || !category || !subject || !message) {
      return res.status(400).json({ error: "Missing required ticket parameters" });
    }

    const initialMessages = [{
      sender: "user",
      senderName: name,
      text: message,
      createdAt: new Date()
    }];

    const ticket = await Ticket.create({
      id,
      name,
      email,
      category,
      subject,
      message,
      status: "OPEN",
      messages: initialMessages
    });

    res.json(ticket);
  } catch (error) {
    console.error("Failed to create ticket:", error);
    res.status(500).json({ error: "Failed to create support ticket" });
  }
});

app.get("/api/tickets", async (req, res) => {
  try {
    const { email } = req.query;
    let userEmail = email;

    // Check with Authorization Header if possible
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    if (token) {
      try {
        const decoded = jwt.verify(token, JWT_SECRET);
        userEmail = decoded.email;
        // Admin gets ALL tickets or optionally matching the email query parameter
        if (decoded.role === "admin") {
          const tickets = email ? await Ticket.find({ email }) : await Ticket.find({});
          return res.json(tickets);
        }
      } catch (e) {
        // Continue with query string if token fails
      }
    }

    if (!userEmail) {
      return res.status(400).json({ error: "Email target is required to view tickets" });
    }

    const tickets = await Ticket.find({ email: userEmail });
    res.json(tickets);
  } catch (error) {
    console.error("Failed to fetch tickets:", error);
    res.status(500).json({ error: "Failed to fetch support tickets" });
  }
});

app.post("/api/tickets/:id/messages", async (req, res) => {
  try {
    const { id } = req.params;
    const { sender, senderName, text } = req.body;

    if (!sender || !text) {
      return res.status(400).json({ error: "Sender and text are required" });
    }

    const ticket = await Ticket.findOne({ id });
    if (!ticket) {
      return res.status(404).json({ error: "Ticket not found" });
    }

    ticket.messages = ticket.messages || [];
    ticket.messages.push({
      sender,
      senderName: senderName || sender,
      text,
      createdAt: new Date()
    });

    await ticket.save();
    res.json(ticket);
  } catch (error) {
    console.error("Failed to append chat message:", error);
    res.status(500).json({ error: "Failed to add support response" });
  }
});

app.put("/api/tickets/:id/status", async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;

    const ticket = await Ticket.findOne({ id });
    if (!ticket) {
      return res.status(404).json({ error: "Ticket not found" });
    }

    ticket.status = status;
    await ticket.save();
    res.json(ticket);
  } catch (error) {
    console.error("Failed to update status:", error);
    res.status(500).json({ error: "Failed to update ticket status" });
  }
});

app.delete("/api/tickets/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const ticket = await Ticket.findOne({ id });
    if (ticket) {
      await Ticket.findByIdAndDelete(ticket._id);
    }
    res.json({ success: true });
  } catch (error) {
    console.error("Failed to delete ticket:", error);
    res.status(500).json({ error: "Failed to delete support ticket" });
  }
});

// Start server wrapper to avoid top-level awaits and support standard hosted environments
async function startServer() {
  const isProduction = process.env.NODE_ENV === "production" || fs.existsSync(path.join(process.cwd(), "dist"));

  if (!isProduction) {
    console.log("Starting in development mode with Vite middleware...");
    const { createServer: createViteServer } = await import("vite");
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    console.log("Starting in production mode serving static files from dist...");
    app.use(express.static("dist"));
    app.get('*', (req, res) => {
      res.sendFile(path.join(process.cwd(), 'dist', 'index.html'));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

// Start database connection asynchronously for fast port binding to avoid gateway startup timeouts
initializeDatabase();

// Execute server start
startServer().catch(err => {
  console.error("Critical error during server startup:", err);
});

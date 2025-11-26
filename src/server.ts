/**
 * Chat Server for LinkUp Platform 
 * Real-time chat functionality using Socket.IO
 * @module ChatServer
 */
import 'dotenv/config';
import express from 'express';
import { createServer } from 'http';
import { Server, Socket } from 'socket.io';
import cors from 'cors';
import admin from 'firebase-admin';

// Initialize Express app
const app = express();
const server = createServer(app);

// Middleware
app.use(cors({
  origin: process.env.CORS_ORIGIN?.split(',') || [
    'http://localhost:5173',
    'https://link-up-frontend-tau.vercel.app'
  ],
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE']
}));
app.use(express.json());

// Initialize Firebase Admin
const serviceAccount = {
  projectId: process.env.FIREBASE_PROJECT_ID,
  clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
  privateKey: process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n')
};

const firebaseApp = admin.initializeApp({
  credential: admin.credential.cert(serviceAccount as admin.ServiceAccount),
  databaseURL: process.env.FIREBASE_DATABASE_URL
});

const db = firebaseApp.firestore();

// Initialize Socket.IO
const io = new Server(server, {
  cors: {
    origin: process.env.CORS_ORIGIN?.split(',') || [
      'http://localhost:5173',
      'https://link-up-frontend-tau.vercel.app'
    ],
    methods: ['GET', 'POST'],
    credentials: true
  },
  transports: ['websocket', 'polling']
});

// Types
interface User {
  userId: string;
  socketId: string;
  displayName: string;
  email: string;
  joinedAt: string;
}

interface ChatMessage {
  id: string;
  userId: string;
  displayName: string;
  message: string;
  type: 'text' | 'system';
  timestamp: string;
}

// Store active meetings and users
const activeMeetings = new Map<string, Set<User>>();

/**
 * Authentication middleware for Socket.IO connections
 */
const authenticateSocket = async (socket: Socket, next: (err?: Error) => void) => {
  try {
    const token = socket.handshake.auth.token;
    
    if (!token) {
      return next(new Error('Authentication error: No token provided'));
    }

    const decodedToken = await admin.auth().verifyIdToken(token);
    socket.data.user = {
      uid: decodedToken.uid,
      email: decodedToken.email,
      name: decodedToken.name || decodedToken.email?.split('@')[0] || 'User'
    };
    
    next();
  } catch (error) {
    console.error('Socket authentication error:', error);
    next(new Error('Authentication error: Invalid token'));
  }
};

io.use(authenticateSocket);

/**
 * Handle Socket.IO connections
 */
io.on('connection', (socket: Socket) => {
  const user = socket.data.user;
  console.log(`🔗 User connected: ${user.uid} (${user.email})`);

  /**
   * Join a meeting room
   */
  socket.on('join_meeting', async (data: { meetingId: string }) => {
    const { meetingId } = data;
    
    try {
      // Verify meeting exists
      const meetingDoc = await db.collection('meetings').doc(meetingId).get();
      
      if (!meetingDoc.exists) {
        socket.emit('error', { message: 'Meeting not found' });
        return;
      }

      const meeting = meetingDoc.data();
      
      // ✅ PERMITIR ACCESO: Agregar automáticamente como participante
      const isOwner = meeting?.ownerUid === user.uid;
      const isParticipant = meeting?.participants?.includes(user.uid);
      
      // Si no es participante, agregarlo automáticamente (permitir acceso público)
      if (!isParticipant) {
        console.log(`➕ Adding ${user.uid} as participant to meeting ${meetingId}`);
        try {
          await db.collection('meetings').doc(meetingId).update({
            participants: admin.firestore.FieldValue.arrayUnion(user.uid)
          });
        } catch (error) {
          console.error('Error adding participant:', error);
        }
      }

      // Leave any previous meetings first
      for (const [existingMeetingId, users] of activeMeetings.entries()) {
        const existingUser = Array.from(users).find(u => u.userId === user.uid);
        if (existingUser) {
          users.delete(existingUser);
          socket.leave(existingMeetingId);
          console.log(`🔄 User ${user.uid} left previous meeting ${existingMeetingId}`);
        }
      }

      // Join the meeting room
      socket.join(meetingId);
      
      // ✅ CRITICAL: Remove duplicates before adding
      if (!activeMeetings.has(meetingId)) {
        activeMeetings.set(meetingId, new Set());
      }
      
      const meetingUsers = activeMeetings.get(meetingId)!;
      
      // Remove any existing instance of this user (by userId, not socketId)
      for (const existingUser of Array.from(meetingUsers)) {
        if (existingUser.userId === user.uid) {
          meetingUsers.delete(existingUser);
          console.log(`🔄 Removed duplicate user entry for ${user.uid}`);
        }
      }
      
      const userData: User = {
        userId: user.uid,
        socketId: socket.id,
        displayName: user.name,
        email: user.email,
        joinedAt: new Date().toISOString()
      };
      
      meetingUsers.add(userData);

      // Get current participants
      const participants = Array.from(activeMeetings.get(meetingId) || []);
      
      // ✅ Notify OTHERS about new user (no al que se une)
      socket.to(meetingId).emit('user_joined', {
        userId: user.uid,
        displayName: user.name,
        email: user.email,
        participants: participants, // Lista completa
        participantsCount: participants.length,
        timestamp: new Date().toISOString()
      });

      // ✅ Send current participants to the NEW user
      socket.emit('meeting_joined', {
        meetingId,
        participants,
        participantsCount: participants.length,
        meetingTitle: meeting?.title || 'Untitled Meeting'
      });

      console.log(`👥 User ${user.uid} joined meeting ${meetingId} (${participants.length} participants)`);

    } catch (error) {
      console.error('Error joining meeting:', error);
      socket.emit('error', { message: 'Failed to join meeting' });
    }
  });

  /**
   * Send chat message - ✅ CORREGIDO: no duplicar
   */
  socket.on('send_message', async (data: {
    meetingId: string;
    message: string;
    type?: 'text' | 'system';
  }) => {
    const { meetingId, message, type = 'text' } = data;

    try {
      // Verify user is in the meeting
      const meetingUsers = activeMeetings.get(meetingId);
      if (!meetingUsers || !Array.from(meetingUsers).some(u => u.socketId === socket.id)) {
        socket.emit('error', { message: 'Not in meeting' });
        return;
      }

      const messageData: ChatMessage = {
        id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        userId: user.uid,
        displayName: user.name,
        message: message.trim(),
        type,
        timestamp: new Date().toISOString()
      };

      // ✅ CORRECCIÓN: Enviar a TODA la sala (incluyendo emisor)
      // Socket.IO maneja esto correctamente con io.to()
      io.to(meetingId).emit('new_message', messageData);
      
      // Store message in Firestore
      await storeMessageInFirestore(meetingId, messageData);
      
      console.log(`💬 Message sent in meeting ${meetingId} by ${user.uid}`);

    } catch (error) {
      console.error('Error sending message:', error);
      socket.emit('error', { message: 'Failed to send message' });
    }
  });

  /**
   * Handle user typing start
   */
  socket.on('typing_start', (data: { meetingId: string }) => {
    const { meetingId } = data;
    
    // ✅ Solo notificar a OTROS (no a quien está escribiendo)
    socket.to(meetingId).emit('user_typing', {
      userId: user.uid,
      displayName: user.name
    });
  });

  /**
   * Handle user typing stop
   */
  socket.on('typing_stop', (data: { meetingId: string }) => {
    const { meetingId } = data;
    
    socket.to(meetingId).emit('user_stop_typing', {
      userId: user.uid
    });
  });

  /**
   * Handle disconnect
   */
  socket.on('disconnect', () => {
    console.log(`🔌 User disconnected: ${user.uid}`);
    
    // Remove user from all meetings
    for (const [meetingId, users] of activeMeetings.entries()) {
      const userToRemove = Array.from(users).find(u => u.socketId === socket.id);
      
      if (userToRemove) {
        users.delete(userToRemove);
        
        const remainingParticipants = Array.from(users);
        
        // Notify others about user leaving
        socket.to(meetingId).emit('user_left', {
          userId: user.uid,
          displayName: user.name,
          participants: remainingParticipants,
          participantsCount: users.size,
          timestamp: new Date().toISOString()
        });

        console.log(`👋 User ${user.uid} left meeting ${meetingId} (${users.size} participants left)`);

        // Clean up empty meetings
        if (users.size === 0) {
          activeMeetings.delete(meetingId);
          console.log(`🗑️ Meeting ${meetingId} cleaned up (no participants)`);
        }
      }
    }
  });
});

/**
 * Store message in Firestore for history
 */
async function storeMessageInFirestore(meetingId: string, messageData: ChatMessage): Promise<void> {
  try {
    await db.collection('meetings').doc(meetingId).collection('messages').add({
      ...messageData,
      storedAt: new Date().toISOString()
    });
  } catch (error) {
    console.error('Error storing message in Firestore:', error);
  }
}

/**
 * Health check endpoint
 */
app.get('/api/health', (req, res) => {
  res.json({
    success: true,
    status: 'OK',
    service: 'LinkUp Chat Server',
    timestamp: new Date().toISOString(),
    activeMeetings: activeMeetings.size,
    totalParticipants: Array.from(activeMeetings.values()).reduce(
      (total, users) => total + users.size, 0
    ),
    version: '1.0.0'
  });
});

/**
 * Get meeting participants
 */
app.get('/api/meetings/:meetingId/participants', async (req, res) => {
  try {
    const { meetingId } = req.params;
    const participants = Array.from(activeMeetings.get(meetingId) || []);
    
    res.json({
      success: true,
      participants,
      count: participants.length
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to get participants'
    });
  }
});

app.use('*', (req, res) => {
  res.status(404).json({
    success: false,
    error: 'Route not found',
    path: req.originalUrl
  });
});

app.use((
  err: any,
  req: express.Request,
  res: express.Response,
  next: express.NextFunction
) => {
  console.error('Server error:', err);
  res.status(500).json({
    success: false,
    error: process.env.NODE_ENV === 'production' ? 'Internal server error' : err.message
  });
});

const PORT = process.env.PORT || 3001;

server.listen(PORT, () => {
  console.log(`🚀 LinkUp Chat Server running on port ${PORT}`);
  console.log(`📊 Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/api/health`);
  console.log(`👥 CORS enabled for: ${process.env.CORS_ORIGIN}`);
});

export default server;
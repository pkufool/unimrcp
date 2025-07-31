#!/usr/bin/env python3
"""
Simple WebSocket ASR Backend for Testing UniMRCP Demo Recognition Engine

This is a mock ASR service that demonstrates the WebSocket JSON protocol
required by the enhanced demo recognition engine.

Usage:
    python3 test_asr_backend.py [--host HOST] [--port PORT] [--path PATH]

Dependencies:
    pip install websockets asyncio
"""

import asyncio
import websockets
import json
import logging
import argparse
import time
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MockASRBackend:
    def __init__(self):
        self.recognition_active = False
        self.audio_buffer = bytearray()
        
    async def handle_client(self, websocket, path):
        """Handle incoming WebSocket connection from UniMRCP plugin"""
        logger.info(f"New client connected from {websocket.remote_address}")
        
        try:
            self.recognition_active = True
            self.audio_buffer.clear()
            
            # Start mock recognition process
            asyncio.create_task(self.mock_recognition(websocket))
            
            # Handle incoming audio data
            async for message in websocket:
                if isinstance(message, bytes):
                    # Received audio data
                    self.audio_buffer.extend(message)
                    logger.debug(f"Received {len(message)} bytes of audio data")
                    
                    # Simulate processing trigger after receiving some audio
                    if len(self.audio_buffer) > 8000:  # ~1 second at 8kHz
                        await self.send_partial_result(websocket)
                        
        except websockets.exceptions.ConnectionClosed:
            logger.info("Client disconnected")
        except Exception as e:
            logger.error(f"Error handling client: {e}")
        finally:
            self.recognition_active = False
            
    async def mock_recognition(self, websocket):
        """Simulate ASR recognition process with partial and final results"""
        
        # Wait a bit to simulate processing
        await asyncio.sleep(0.5)
        
        # Send partial results
        partial_results = [
            "hello",
            "hello wor",
            "hello world", 
            "hello world this",
            "hello world this is",
            "hello world this is a"
        ]
        
        for partial in partial_results:
            if not self.recognition_active:
                break
                
            await self.send_partial_result(websocket, partial)
            await asyncio.sleep(0.3)  # Simulate processing time
            
        # Send final result
        if self.recognition_active:
            await asyncio.sleep(0.5)
            await self.send_final_result(websocket, "hello world this is a test")
            
    async def send_partial_result(self, websocket, text=None):
        """Send partial recognition result"""
        if text is None:
            # Generate random partial result
            words = ["hello", "world", "this", "is", "a", "test", "recognition"]
            text = " ".join(random.sample(words, random.randint(1, 3)))
            
        message = {
            "type": "partial",
            "result": text
        }
        
        try:
            await websocket.send(json.dumps(message))
            logger.info(f"Sent partial result: {text}")
        except Exception as e:
            logger.error(f"Error sending partial result: {e}")
            
    async def send_final_result(self, websocket, text):
        """Send final recognition result"""
        message = {
            "type": "final", 
            "result": text
        }
        
        try:
            await websocket.send(json.dumps(message))
            logger.info(f"Sent final result: {text}")
        except Exception as e:
            logger.error(f"Error sending final result: {e}")

def main():
    parser = argparse.ArgumentParser(description='Mock ASR WebSocket Backend')
    parser.add_argument('--host', default='localhost', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8080, help='Port to bind to')
    parser.add_argument('--path', default='/asr', help='WebSocket path')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create ASR backend
    asr_backend = MockASRBackend()
    
    logger.info(f"Starting mock ASR backend on {args.host}:{args.port}{args.path}")
    
    # Start WebSocket server
    start_server = websockets.serve(
        asr_backend.handle_client,
        args.host,
        args.port,
        subprotocols=['asr-protocol']
    )
    
    # Run server
    asyncio.get_event_loop().run_until_complete(start_server)
    logger.info("Mock ASR backend started successfully")
    
    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down mock ASR backend")

if __name__ == '__main__':
    main()
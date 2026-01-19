#!/usr/bin/env python3

import os
import subprocess
import sys

Import("env")

def upload_littlefs(source, target, env):
  """
  Upload LittleFS filesystem image to ESP32
  """
  print("=" * 60)
  print("Creating and uploading LittleFS image...")
  print("=" * 60)
  
  #-- Get platformio environment variables
  platform = env.PioPlatform()
  board = env.BoardConfig()
  
  #-- Build the filesystem image first
  print("\n[1/2] Building filesystem image from 'data' folder...")
  ret = env.Execute("pio run --target buildfs --environment " + env["PIOENV"])
  
  if ret != 0:
    print("ERROR: Failed to build filesystem image")
    sys.exit(1)
  
  #-- Upload the filesystem image
  print("\n[2/2] Uploading filesystem image to ESP32...")
  ret = env.Execute("pio run --target uploadfs --environment " + env["PIOENV"])
  
  if ret != 0:
    print("ERROR: Failed to upload filesystem image")
    sys.exit(1)
  
  print("\n" + "=" * 60)
  print("LittleFS upload completed successfully!")
  print("=" * 60)

#-- Create custom target
env.AlwaysBuild(env.Alias("uploadlfs", None, upload_littlefs))

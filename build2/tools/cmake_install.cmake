# Install script for directory: /home/jaking/bustub/tools

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/home/jaking/bustub/build/tools/shell/cmake_install.cmake")
  include("/home/jaking/bustub/build/tools/nc-shell/cmake_install.cmake")
  include("/home/jaking/bustub/build/tools/sqllogictest/cmake_install.cmake")
  include("/home/jaking/bustub/build/tools/wasm-shell/cmake_install.cmake")
  include("/home/jaking/bustub/build/tools/b_plus_tree_printer/cmake_install.cmake")
  include("/home/jaking/bustub/build/tools/wasm-bpt-printer/cmake_install.cmake")
  include("/home/jaking/bustub/build/tools/terrier_bench/cmake_install.cmake")
  include("/home/jaking/bustub/build/tools/bpm_bench/cmake_install.cmake")
  include("/home/jaking/bustub/build/tools/btree_bench/cmake_install.cmake")
  include("/home/jaking/bustub/build/tools/htable_bench/cmake_install.cmake")

endif()


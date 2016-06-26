//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// lockcmds.h
//
// Identification: src/include/parser/commands/lockcmds.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


/*-------------------------------------------------------------------------
 *
 * lockcmds.h
 *	  prototypes for lockcmds.c.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/lockcmds.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCKCMDS_H
#define LOCKCMDS_H

#include "parser/nodes/parsenodes.h"

/*
 * LOCK
 */
extern void LockTableCommand(LockStmt *lockstmt);

#endif   /* LOCKCMDS_H */
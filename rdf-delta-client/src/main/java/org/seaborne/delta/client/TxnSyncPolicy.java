/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seaborne.delta.client;

/**
 * When to synchronize with a patch log.
 * {@link DeltaConnection} provide the option of syncing automagtically on transaction begin.
 * The application call also call {@link DeltaConnection#sync} itself. 
 * <ul>
 * <li>{@code NONE} No automatic sync, all done by the application.
 * <li>{@code TXN_RW} When a transaction starts (sync attempt for a READ transaction suppresses network errors). 
 * <li>{@code TXN_RW} When a wite-transaction starts. 
 * </ul>
 */
public enum TxnSyncPolicy { NONE, TXN_RW, TXN_W }
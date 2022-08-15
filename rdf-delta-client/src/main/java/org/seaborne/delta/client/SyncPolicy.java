/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.client;

/**
 * When to synchronize with a patch log.
 * {@link DeltaConnection} provides the option of syncing automatically on transaction begin.
 * The application call also call {@link DeltaConnection#sync} itself.
 * <ul>
 * <li>{@code NONE} No automatic sync, all done by the application.
 * <li>{@code TXN_RW} When a transaction starts (sync attempt for a READ transaction suppresses network errors).
 * <li>{@code TXN_W} When a write-transaction starts.
 * </ul>
 */
public enum SyncPolicy { NONE, TXN_RW, TXN_W }

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

package org.seaborne.delta.cmds;

import org.apache.jena.cmd.CmdException;
import org.apache.jena.atlas.json.JsonException;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.web.HttpException;

/** List a new log */
public class pingserver extends DeltaCmd {

    public static void main(String... args) {
        new pingserver(args).mainRun();
    }

    public pingserver(String[] argv) {
        super(argv) ;
        super.add(argDataSourceURI);
    }

    @Override
    protected String getSummary() {
        return getCommandName()+" --server URL";
    }

    @Override
    protected void execCmd() {
        ping();
    }

    protected void ping() {
        try {
            JsonObject obj = dLink.ping();
            System.out.println(obj);
        } catch (HttpException ex) {
            throw new CmdException(messageFromHttpException(ex));
        } catch (JsonException ex) {
            throw new CmdException("Not an RDF Patch server");
        }
    }

    @Override
    protected void checkForMandatoryArgs() {}
}

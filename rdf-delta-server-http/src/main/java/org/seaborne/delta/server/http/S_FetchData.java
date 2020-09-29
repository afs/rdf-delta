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

package org.seaborne.delta.server.http;

import java.io.FileNotFoundException ;
import java.io.IOException;
import java.io.InputStream ;
import java.nio.file.Files ;
import java.nio.file.NoSuchFileException ;
import java.nio.file.Path ;
import java.nio.file.Paths ;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils ;
import org.apache.jena.atlas.lib.IRILib ;
import org.apache.jena.atlas.web.ContentType ;
import org.apache.jena.riot.RDFLanguages ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaNotFoundException ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.server.local.DataSource;
import org.slf4j.Logger ;

/** Data over HTTP. */
public class S_Data extends HttpOperationBase {
    static private Logger LOG = Delta.getDeltaLogger("Data") ;

    public S_Data(DeltaLink engine) {
        super(engine);
    }

    // Fetch a file
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }

    @Override
    protected void validateAction(Args httpArgs) {
    }

    @Override
    protected void executeAction(DeltaAction action) throws IOException {
        LOG.info("GET "+action.getURL());
        Id dsRef = Id.fromString(action.httpArgs.datasourceName);
        String filenameIRI = determineData(action, dsRef);
        ContentType ct = RDFLanguages.guessContentType(filenameIRI) ;
        String fn = IRILib.IRIToFilename(filenameIRI);
        Path path = Paths.get(fn);
        try ( InputStream in = Files.newInputStream(path) ) {
            action.response.setStatus(HttpSC.OK_200);
            action.response.setContentType(ct.getContentTypeStr());
            IOUtils.copy(in, action.response.getOutputStream());
        } catch (NoSuchFileException | FileNotFoundException ex) {
            throw new DeltaNotFoundException(action.getURL());
        }
    }

    /** Decide which data to return.
     *  Default is the initial data for a {@link DataSource}.
     */
    protected String determineData(DeltaAction action, Id dsRef) {
        return action.dLink.initialState(dsRef);
    }

    @Override
    protected String getOpName() {
        return "data";
    }
}
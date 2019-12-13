package io.siddhi.distribution.event.simulator.core.impl;

import io.siddhi.distribution.common.common.exception.ResponseMapper;
import io.siddhi.distribution.event.simulator.core.api.FilesApiService;
import io.siddhi.distribution.event.simulator.core.api.NotFoundException;
import io.siddhi.distribution.event.simulator.core.exception.FileAlreadyExistsException;
import io.siddhi.distribution.event.simulator.core.exception.FileOperationsException;
import io.siddhi.distribution.event.simulator.core.exception.InvalidFileException;
import io.siddhi.distribution.event.simulator.core.internal.generator.csv.util.FileUploader;
import io.siddhi.distribution.event.simulator.core.internal.util.CommonOperations;
import io.siddhi.distribution.event.simulator.core.internal.util.EventSimulatorConstants;
import org.apache.commons.io.FilenameUtils;
import org.wso2.carbon.utils.Utils;
import org.wso2.msf4j.Request;
import org.wso2.msf4j.formparam.FileInfo;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.ws.rs.core.Response;

/**
 * File API service implementation class.
 */
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaMSF4JServerCodegen",
        date = "2017-07-20T09:30:14.336Z")
public class FilesApiServiceImpl extends FilesApiService {
    private static final Path CSV_BASE_PATH = Paths.get(Utils.getRuntimePath().toString(),
            EventSimulatorConstants.DIRECTORY_DEPLOYMENT, EventSimulatorConstants.DIRECTORY_CSV_FILES);

    public Response deleteFile(String fileName) throws NotFoundException, FileOperationsException {
        FileUploader fileUploader = FileUploader.getFileUploaderInstance();
        CommonOperations.validatePath(fileName);
        if (FilenameUtils.isExtension(fileName, EventSimulatorConstants.CSV_FILE_EXTENSION)) {
            boolean deleted = false;
            try {
                deleted = fileUploader.deleteFile(fileName, CSV_BASE_PATH.toString());
            } catch (FileOperationsException e) {
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .header("Access-Control-Allow-Origin", "*")
                        .entity(new ResponseMapper(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage()))
                        .build();
            }
            if (deleted) {
                return Response.ok()
                        .header("Access-Control-Allow-Origin", "*")
                        .entity(new ResponseMapper(Response.Status.OK, "Successfully deleted file '" +
                                fileName + "'"))
                        .build();
            } else {
                return Response.status(Response.Status.NOT_FOUND)
                        .header("Access-Control-Allow-Origin", "*")
                        .entity(new ResponseMapper(Response.Status.NOT_FOUND, "File '" + fileName +
                                "' does not exist"))
                        .build();
            }
        } else {
            return Response.status(Response.Status.NOT_FOUND)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity(new ResponseMapper(Response.Status.NOT_FOUND, "File '" + fileName +
                            "' is not a CSV file."))
                    .build();
        }
    }

    public Response getFileNames() throws NotFoundException {
        try {
            return Response.status(Response.Status.OK)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity(
                            FileUploader.getFileUploaderInstance()
                                    .retrieveFileNameList(EventSimulatorConstants.CSV_FILE_EXTENSION, CSV_BASE_PATH)
                    )
                    .build();
        } catch (FileOperationsException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity(new ResponseMapper(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage()))
                    .build();
        }
    }

    public Response updateFile(String fileName, InputStream fileInputStream, FileInfo fileInfo)
            throws NotFoundException, FileOperationsException {
        CommonOperations.validatePath(fileName);
        FileUploader fileUploader = FileUploader.getFileUploaderInstance();
        if (FilenameUtils.isExtension(fileName, EventSimulatorConstants.CSV_FILE_EXTENSION)) {
            if (FilenameUtils.isExtension(fileInfo.getFileName(), EventSimulatorConstants.CSV_FILE_EXTENSION)) {
                if (fileUploader.validateFileExists(fileName)) {
                    boolean deleted = false;
                    try {
                        deleted = fileUploader.deleteFile(fileName, CSV_BASE_PATH.toString());
                    } catch (FileOperationsException e) {
                        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                .header("Access-Control-Allow-Origin", "*")
                                .entity(new ResponseMapper(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage()))
                                .build();
                    }
                    if (deleted) {
                        try {
                            fileUploader.uploadFile(fileInfo, fileInputStream, CSV_BASE_PATH.toString());
                        } catch (FileAlreadyExistsException | FileOperationsException | InvalidFileException e) {
                            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                    .header("Access-Control-Allow-Origin", "*")
                                    .entity(new ResponseMapper(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage()))
                                    .build();
                        }
                        return Response.ok()
                                .header("Access-Control-Allow-Origin", "*")
                                .entity(new ResponseMapper(Response.Status.OK, "Successfully updated CSV" +
                                        " file '" + fileName + "' with file '" + fileInfo.getFileName() + "'."))
                                .build();
                    } else {
                        return Response.status(Response.Status.NOT_FOUND)
                                .header("Access-Control-Allow-Origin", "*")
                                .entity(new ResponseMapper(Response.Status.NOT_FOUND, "File '" + fileName +
                                        "' does not exist"))
                                .build();
                    }
                } else {
                    return Response.status(Response.Status.BAD_REQUEST)
                            .header("Access-Control-Allow-Origin", "*")
                            .entity(new ResponseMapper(Response.Status.NOT_FOUND, "File '" + fileName + "' " +
                                    "does not exist."))
                            .build();
                }
            } else {
                return Response.status(Response.Status.BAD_REQUEST)
                        .header("Access-Control-Allow-Origin", "*")
                        .entity(new ResponseMapper(Response.Status.BAD_REQUEST, "File '" + fileName + "' is" +
                                " not a CSV file"))
                        .build();
            }
        } else {
            return Response.status(Response.Status.BAD_REQUEST)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity(new ResponseMapper(Response.Status.BAD_REQUEST, "File '" + fileName + "' is" +
                            " not a CSV file"))
                    .build();
        }
    }

    public Response uploadFile(InputStream fileInputStream, FileInfo fileInfo)
            throws NotFoundException, FileOperationsException {
        CommonOperations.validatePath(fileInfo.getFileName());
        try {
            FileUploader.getFileUploaderInstance().uploadFile(fileInfo, fileInputStream, CSV_BASE_PATH.toString());
        } catch (FileAlreadyExistsException | FileOperationsException | InvalidFileException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .header("Access-Control-Allow-Origin", "*")
                    .entity(new ResponseMapper(Response.Status.INTERNAL_SERVER_ERROR, e.getMessage()))
                    .build();
        }
        return Response.status(Response.Status.CREATED)
                .header("Access-Control-Allow-Origin", "*")
                .entity(new ResponseMapper(Response.Status.CREATED, "Successfully uploaded file '" +
                        fileInfo.getFileName() + "'"))
                .build();
    }

    @Override
    public Response deleteFile(String fileName, Request request) throws NotFoundException, FileOperationsException {
        return deleteFile(fileName);
    }

    @Override
    public Response getFileNames(Request request) throws NotFoundException {
        return getFileNames();
    }

    @Override
    public Response updateFile(String fileName, InputStream fileInputStream, FileInfo fileDetail, Request request)
            throws NotFoundException, FileOperationsException {
        return updateFile(fileName, fileInputStream, fileDetail);
    }

    @Override
    public Response uploadFile(InputStream fileInputStream, FileInfo fileDetail, Request request)
            throws NotFoundException, FileOperationsException {
        return uploadFile(fileInputStream, fileDetail);
    }
}

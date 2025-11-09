# -*- coding: utf-8 -*-
from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingParameterRasterLayer,
    QgsProcessingParameterFolderDestination,
    QgsProcessingParameterEnum,
    QgsProcessingContext,
    QgsProcessingException,
    QgsProject,
)
from qgis import processing
import os
from osgeo import gdal


class PostprocReprojectAndCSV_Multi(QgsProcessingAlgorithm):
    RASTER_IN = "RASTER_IN"
    OUT_FOLDER = "OUT_FOLDER"
    OUT_DTYPE = "OUT_DTYPE"

    def tr(self, text):
        return QCoreApplication.translate("PostprocReprojectAndCSV_Multi", text)

    def createInstance(self):
        return PostprocReprojectAndCSV_Multi()

    def name(self):
        return "postproc_reproj_xyz_dtype_multi"

    def displayName(self):
        return self.tr("Postproceso: reproyectar y exportar a CSV (XY + todas las bandas)")

    def group(self):
        return self.tr("Geología / Teledetección")

    def groupId(self):
        return "geologia_teledeteccion"

    def shortHelpString(self):
        return self.tr(
            "Toma un raster multibanda (3, 4, 5, 7, …), lo reproyecta al SRC del proyecto con el tipo de dato elegido "
            "y exporta un CSV con X, Y y TODAS las bandas."
        )

    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterRasterLayer(
                self.RASTER_IN,
                self.tr("Raster multibanda (alteraciones / stacked)"),
            )
        )

        self.addParameter(
            QgsProcessingParameterFolderDestination(
                self.OUT_FOLDER,
                self.tr("Carpeta de salida"),
            )
        )

        dtype_options = [
            self.tr("Mantener (Float64)"),
            self.tr("Float32"),
            self.tr("Int16"),
            self.tr("UInt16"),
            self.tr("Byte"),
        ]
        self.addParameter(
            QgsProcessingParameterEnum(
                self.OUT_DTYPE,
                self.tr("Tipo de dato de salida"),
                options=dtype_options,
                defaultValue=1,  # Float32
            )
        )

    def processAlgorithm(self, parameters, context: QgsProcessingContext, feedback):
        ras_lyr = self.parameterAsRasterLayer(parameters, self.RASTER_IN, context)
        out_folder = self.parameterAsString(parameters, self.OUT_FOLDER, context)
        dtype_idx = self.parameterAsEnum(parameters, self.OUT_DTYPE, context)

        if ras_lyr is None:
            raise QgsProcessingException("No se entregó raster de entrada.")

        if out_folder == "TEMPORARY_OUTPUT":
            out_folder = context.temporaryDirectory()
        os.makedirs(out_folder, exist_ok=True)

        # map de opción → código GDAL
        if dtype_idx == 0:
            data_type = 6  # Float64
        elif dtype_idx == 1:
            data_type = 5  # Float32
        elif dtype_idx == 2:
            data_type = 2  # Int16
        elif dtype_idx == 3:
            data_type = 3  # UInt16
        elif dtype_idx == 4:
            data_type = 1  # Byte
        else:
            data_type = 6

        # CRS del proyecto
        proj = context.project() or QgsProject.instance()
        proj_crs = proj.crs()
        if not proj_crs.isValid():
            raise QgsProcessingException("El proyecto no tiene un SRC válido.")

        feedback.pushInfo(f"Reproyectando a CRS del proyecto: {proj_crs.authid()}")

        base_name = os.path.splitext(os.path.basename(ras_lyr.source()))[0]
        reproj_path = os.path.join(out_folder, base_name + "_reproj.tif")

        # reproyectar con bilinear
        processing.run(
            "gdal:warpreproject",
            {
                "INPUT": ras_lyr.source(),
                "SOURCE_CRS": ras_lyr.crs().toWkt(),
                "TARGET_CRS": proj_crs.toWkt(),
                "RESAMPLING": 1,  # bilinear
                "NODATA": None,
                "TARGET_RESOLUTION": None,
                "OPTIONS": "",
                "DATA_TYPE": data_type,
                "TARGET_EXTENT": None,
                "TARGET_EXTENT_CRS": None,
                "MULTITHREADING": True,
                "OUTPUT": reproj_path,
            },
            context=context,
            feedback=feedback,
        )

        feedback.pushInfo(f"Raster reproyectado: {reproj_path}")

        # ahora lo abrimos con GDAL para sacar todas las bandas
        ds = gdal.Open(reproj_path, gdal.GA_ReadOnly)
        if ds is None:
            raise QgsProcessingException("No se pudo abrir el raster reproyectado.")

        gt = ds.GetGeoTransform()
        xsize = ds.RasterXSize
        ysize = ds.RasterYSize
        band_count = ds.RasterCount

        if band_count == 0:
            raise QgsProcessingException("El raster no tiene bandas.")

        # preparamos bandas, nodata y nombres
        bands = []
        headers = ["x", "y"]
        for b in range(1, band_count + 1):
            rb = ds.GetRasterBand(b)
            bands.append(rb)
            name = rb.GetDescription() or f"band{b}"
            headers.append(name)

        csv_path = os.path.join(out_folder, base_name + "_reproj_xyz.csv")
        feedback.pushInfo(f"Exportando CSV: {csv_path}")

        with open(csv_path, "w", encoding="utf-8") as f:
            f.write(",".join(headers) + "\n")

            for j in range(ysize):
                if feedback.isCanceled():
                    break

                # leemos todas las bandas de esta fila
                row_vals = []
                for rb in bands:
                    arr = rb.ReadAsArray(0, j, xsize, 1)[0]
                    row_vals.append(arr)

                ypos = gt[3] + (j + 0.5) * gt[5]

                for i in range(xsize):
                    xpos = gt[0] + (i + 0.5) * gt[1] + (j + 0.5) * gt[2]

                    # chequeo nodata: si TODAS las bandas son nodata, no escribo
                    all_nd = True
                    vals = []
                    for idx, rb in enumerate(bands):
                        v = float(row_vals[idx][i])
                        nd = rb.GetNoDataValue()
                        vals.append(v)
                        if nd is None or v != nd:
                            all_nd = False

                    if all_nd:
                        continue

                    # escribo
                    f.write(f"{xpos},{ypos}," + ",".join(str(v) for v in vals) + "\n")

                feedback.setProgress(int(100 * j / max(1, ysize - 1)))

        ds = None
        feedback.pushInfo("Listo ✅")

        return {
            "REPROJECTED_RASTER": reproj_path,
            "CSV": csv_path,
        }

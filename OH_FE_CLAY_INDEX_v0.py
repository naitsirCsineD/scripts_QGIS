# -*- coding: utf-8 -*-
from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingParameterRasterLayer,
    QgsProcessingParameterNumber,
    QgsProcessingParameterFolderDestination,
    QgsProcessingParameterExtent,
    QgsProcessingContext,
    QgsProcessingException,
    QgsCoordinateTransform,
    QgsCoordinateReferenceSystem,
    QgsRectangle,
)
from qgis import processing
import os
import time


class MineralIndicesFromLandsatIllum(QgsProcessingAlgorithm):
    B2 = "B2"
    B4 = "B4"
    B5 = "B5"
    B6 = "B6"
    B7 = "B7"
    DTM = "DTM"
    SUN_AZ = "SUN_AZ"
    SUN_EL = "SUN_EL"
    OUT_FOLDER = "OUT_FOLDER"
    EXTENT = "EXTENT"

    def tr(self, text):
        return QCoreApplication.translate("MineralIndicesFromLandsatIllum", text)

    def createInstance(self):
        return MineralIndicesFromLandsatIllum()

    def name(self):
        return "indices_minerales_landsat_dtm_illum"

    def displayName(self):
        return self.tr("Índices minerales  Fe, OH y Arcillas (Landsat + DTM + corrección de iluminación)")

    def group(self):
        return self.tr("Geología / Teledetección")

    def groupId(self):
        return "geologia_teledeteccion"

    def shortHelpString(self):
        return self.tr(
            "Calcula NDVI, Fe, arcillas (SWIR1/SWIR2) y OH (SWIR1/NIR) a partir de Landsat.\n"
            "Corrige las bandas por iluminación derivada de un DTM (pendiente+aspecto).\n"
            "El DTM puede estar en otro CRS: el script lo reproyecta al CRS de las bandas.\n"
            "El extent del usuario se reproyecta al CRS de las bandas para evitar valores enormes."
        )

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterRasterLayer(self.B2, self.tr("B2 (BLUE)")))
        self.addParameter(QgsProcessingParameterRasterLayer(self.B4, self.tr("B4 (RED)")))
        self.addParameter(QgsProcessingParameterRasterLayer(self.B5, self.tr("B5 (NIR)")))
        self.addParameter(QgsProcessingParameterRasterLayer(self.B6, self.tr("B6 (SWIR1)")))
        self.addParameter(QgsProcessingParameterRasterLayer(self.B7, self.tr("B7 (SWIR2)")))
        self.addParameter(QgsProcessingParameterRasterLayer(self.DTM, self.tr("DTM")))

        self.addParameter(
            QgsProcessingParameterNumber(
                self.SUN_AZ,
                self.tr("Acimut del sol (grados)"),
                type=QgsProcessingParameterNumber.Double,
                defaultValue=135.0,
            )
        )
        self.addParameter(
            QgsProcessingParameterNumber(
                self.SUN_EL,
                self.tr("Elevación del sol (grados)"),
                type=QgsProcessingParameterNumber.Double,
                defaultValue=42.0,
            )
        )

        self.addParameter(
            QgsProcessingParameterExtent(
                self.EXTENT,
                self.tr("Extensión a usar (se reproyecta al CRS de las bandas)"),
                optional=True,
            )
        )

        self.addParameter(
            QgsProcessingParameterFolderDestination(
                self.OUT_FOLDER, self.tr("Carpeta de salida")
            )
        )

    # ---------- helpers ----------
    def _unique(self, out_folder, base):
        # evita WinError 32 y sobreescritura en GDrive
        ts = time.strftime("%Y%m%d_%H%M%S")
        return os.path.join(out_folder, f"{base}_{ts}.tif")

    def _reproject_extent_rect(self, rect: QgsRectangle, src_crs, dest_crs, context):
        """
        Reproyecta un QgsRectangle de src_crs a dest_crs
        """
        if src_crs == dest_crs:
            return rect

        x_min = rect.xMinimum()
        x_max = rect.xMaximum()
        y_min = rect.yMinimum()
        y_max = rect.yMaximum()

        # transformamos las 4 esquinas
        tr = QgsCoordinateTransform(src_crs, dest_crs, context.transformContext())
        p1 = tr.transform(x_min, y_min)
        p2 = tr.transform(x_min, y_max)
        p3 = tr.transform(x_max, y_min)
        p4 = tr.transform(x_max, y_max)

        xs = [p1.x(), p2.x(), p3.x(), p4.x()]
        ys = [p1.y(), p2.y(), p3.y(), p4.y()]
        return QgsRectangle(min(xs), min(ys), max(xs), max(ys))

    # -------------------------------

    def processAlgorithm(self, parameters, context: QgsProcessingContext, feedback):
        # 1. leer capas
        b2_lyr = self.parameterAsRasterLayer(parameters, self.B2, context)
        b4_lyr = self.parameterAsRasterLayer(parameters, self.B4, context)
        b5_lyr = self.parameterAsRasterLayer(parameters, self.B5, context)
        b6_lyr = self.parameterAsRasterLayer(parameters, self.B6, context)
        b7_lyr = self.parameterAsRasterLayer(parameters, self.B7, context)
        dtm_lyr = self.parameterAsRasterLayer(parameters, self.DTM, context)

        if not all([b2_lyr, b4_lyr, b5_lyr, b6_lyr, b7_lyr, dtm_lyr]):
            raise QgsProcessingException("Faltan capas raster de entrada.")

        # el CRS de referencia será el de las bandas
        target_crs = b2_lyr.crs()

        sun_az = self.parameterAsDouble(parameters, self.SUN_AZ, context)
        sun_el = self.parameterAsDouble(parameters, self.SUN_EL, context)

        out_folder = self.parameterAsString(parameters, self.OUT_FOLDER, context)
        if out_folder == "TEMPORARY_OUTPUT":
            out_folder = context.temporaryDirectory()
        if os.path.splitext(out_folder)[1] != "":
            out_folder = os.path.dirname(out_folder)
        os.makedirs(out_folder, exist_ok=True)

        # 2. tomar extent del usuario y reproyectarlo al CRS de las bandas
        # QGIS 3.40 tiene parameterAsExtentCrs
        user_rect, user_crs = None, None
        try:
            user_rect, user_crs = self.parameterAsExtentCrs(parameters, self.EXTENT, context)
        except Exception:
            # fallback: si no hay CRS asociado, usamos el del proyecto
            user_rect = self.parameterAsExtent(parameters, self.EXTENT, context)
            user_crs = context.project().crs() if context.project() else target_crs

        if user_rect is not None and not user_rect.isEmpty():
            feedback.pushInfo("Usando la extensión indicada por el usuario.")
            # reproyectar al CRS de las bandas
            rect_in_target = self._reproject_extent_rect(user_rect, user_crs, target_crs, context)
            common_extent = rect_in_target
        else:
            # intersectar todas las capas EN EL CRS DE LAS BANDAS
            feedback.pushInfo("Usando la intersección de todos los ráster.")
            exts = [
                b2_lyr.extent(),
                b4_lyr.extent(),
                b5_lyr.extent(),
                b6_lyr.extent(),
                b7_lyr.extent(),
            ]
            common_extent = exts[0]
            for e in exts[1:]:
                common_extent = common_extent.intersect(e)
            if common_extent.isEmpty():
                raise QgsProcessingException("Las capas no tienen una intersección común.")

        xmin = common_extent.xMinimum()
        xmax = common_extent.xMaximum()
        ymin = common_extent.yMinimum()
        ymax = common_extent.yMaximum()
        extent_str = f"{xmin},{xmax},{ymin},{ymax}"

        # resolución de las bandas
        resx = b2_lyr.rasterUnitsPerPixelX()
        resy = b2_lyr.rasterUnitsPerPixelY()

        # 3. reproyectar DTM si hace falta
        if dtm_lyr.crs() != target_crs:
            feedback.pushInfo("DTM en CRS distinto, reproyectando al CRS de las Landsat...")
            dtm_reproj = self._unique(out_folder, "dtm_reproj")
            processing.run(
                "gdal:warpreproject",
                {
                    "INPUT": dtm_lyr.source(),
                    "SOURCE_CRS": dtm_lyr.crs().toWkt(),
                    "TARGET_CRS": target_crs.toWkt(),
                    "RESAMPLING": 0,
                    "NODATA": None,
                    "TARGET_RESOLUTION": None,
                    "OPTIONS": "",
                    "DATA_TYPE": 0,
                    "TARGET_EXTENT": None,
                    "TARGET_EXTENT_CRS": None,
                    "MULTITHREADING": True,
                    "OUTPUT": dtm_reproj,
                },
                context=context,
                feedback=feedback,
            )
            dtm_src = dtm_reproj
        else:
            dtm_src = dtm_lyr.source()

        # 4. helper para recortar
        def clip_to_extent(src_path, name):
            outp = self._unique(out_folder, name)
            processing.run(
                "gdal:cliprasterbyextent",
                {
                    "INPUT": src_path,
                    "PROJWIN": extent_str,           # ya está en CRS de las bandas
                    "NODATA": 0,
                    "OPTIONS": "",
                    "DATA_TYPE": 0,
                    "OUTPUT": outp,
                },
                context=context,
                feedback=feedback,
            )
            return outp

        feedback.pushInfo("Recortando capas al extent elegido (CRS de las bandas)...")
        b2_clip = clip_to_extent(b2_lyr.source(), "b2_clip")
        b4_clip = clip_to_extent(b4_lyr.source(), "b4_clip")
        b5_clip = clip_to_extent(b5_lyr.source(), "b5_clip")
        b6_clip = clip_to_extent(b6_lyr.source(), "b6_clip")
        b7_clip = clip_to_extent(b7_lyr.source(), "b7_clip")
        dtm_clip = clip_to_extent(dtm_src, "dtm_clip")

        # 5. pendiente y aspecto
        feedback.pushInfo("Calculando pendiente y aspecto...")
        slope_path = self._unique(out_folder, "slope")
        processing.run(
            "gdal:slope",
            {
                "INPUT": dtm_clip,
                "BAND": 1,
                "SCALE": 1.0,
                "AS_PERCENT": False,
                "COMPUTE_EDGES": True,
                "ZEVENBERGEN": False,
                "OUTPUT": slope_path,
            },
            context=context,
            feedback=feedback,
        )

        aspect_path = self._unique(out_folder, "aspect")
        processing.run(
            "gdal:aspect",
            {
                "INPUT": dtm_clip,
                "BAND": 1,
                "TRIG_ANGLE": False,
                "ZERO_FLAT": False,
                "COMPUTE_EDGES": True,
                "ZEVENBERGEN": False,
                "OUTPUT": aspect_path,
            },
            context=context,
            feedback=feedback,
        )

        # 6. iluminación
        feedback.pushInfo("Calculando iluminación...")
        illum_path = self._unique(out_folder, "illumination")
        pi = 3.14159265
        illum_expr = (
            f"cos({sun_el}*{pi}/180)*cos(A*{pi}/180) + "
            f"sin({sun_el}*{pi}/180)*sin(A*{pi}/180)*cos(({sun_az}-B)*{pi}/180)"
        )
        processing.run(
            "gdal:rastercalculator",
            {
                "INPUT_A": slope_path,
                "BAND_A": 1,
                "INPUT_B": aspect_path,
                "BAND_B": 1,
                "FORMULA": illum_expr,
                "NO_DATA": 0.0,
                "RTYPE": 6,  # Float64
                "EXTRA": "",
                "OPTIONS": "",
                "OUTPUT": illum_path,
            },
            context=context,
            feedback=feedback,
        )

        # 7. alinear iluminación al grid de las bandas
        feedback.pushInfo("Alineando iluminación al grid de las bandas...")
        illum_aligned = self._unique(out_folder, "illumination_aligned")
        processing.run(
            "gdal:warpreproject",
            {
                "INPUT": illum_path,
                "SOURCE_CRS": target_crs.toWkt(),
                "TARGET_CRS": target_crs.toWkt(),
                "RESAMPLING": 0,
                "NODATA": 0.0,
                "TARGET_RESOLUTION": resx,
                "OPTIONS": "",
                "DATA_TYPE": 6,  # Float64
                "TARGET_EXTENT": extent_str,
                "TARGET_EXTENT_CRS": target_crs.toWkt(),
                "MULTITHREADING": True,
                "OUTPUT": illum_aligned,
            },
            context=context,
            feedback=feedback,
        )

        eps = 0.0001

        # helper de corrección por iluminación, con máscara para no dividir por 0 ni por 1.79e+308
        def illum_correct(band_path, name):
            outp = self._unique(out_folder, name)
            # (B > 0.1) evita sombras totales
            # (B < 1e30) evita el 1.79769e+308 que dejó el warp
            formula = f"(B > 0.1) * (B < 1e30) * (A / (B + {eps}))"
            processing.run(
                "gdal:rastercalculator",
                {
                    "INPUT_A": band_path,
                    "BAND_A": 1,
                    "INPUT_B": illum_aligned,
                    "BAND_B": 1,
                    "FORMULA": formula,
                    "NO_DATA": 0.0,
                    "RTYPE": 6,
                    "EXTRA": "",
                    "OPTIONS": "",
                    "OUTPUT": outp,
                },
                context=context,
                feedback=feedback,
            )
            return outp

        feedback.pushInfo("Corrigiendo bandas por iluminación...")
        b2_corr = illum_correct(b2_clip, "b2_corr")
        b4_corr = illum_correct(b4_clip, "b4_corr")
        b5_corr = illum_correct(b5_clip, "b5_corr")
        b6_corr = illum_correct(b6_clip, "b6_corr")
        b7_corr = illum_correct(b7_clip, "b7_corr")

        # helper de dos entradas
        def gdal_calc_2in(a_path, b_path, formula, name):
            outp = self._unique(out_folder, name)
            processing.run(
                "gdal:rastercalculator",
                {
                    "INPUT_A": a_path,
                    "BAND_A": 1,
                    "INPUT_B": b_path,
                    "BAND_B": 1,
                    "FORMULA": formula,
                    "NO_DATA": 0.0,
                    "RTYPE": 6,
                    "EXTRA": "",
                    "OPTIONS": "",
                    "OUTPUT": outp,
                },
                context=context,
                feedback=feedback,
            )
            return outp

        # 8. NDVI seguro
        feedback.pushInfo("Calculando NDVI...")
        ndvi_path = gdal_calc_2in(
            b5_corr,
            b4_corr,
            f"(A + B > 0.0001) * ((A - B) / (A + B + {eps}))",
            "ndvi",
        )

        # 9. índices
        feedback.pushInfo("Calculando índices Fe, Clay, OH...")
        fe_path = gdal_calc_2in(
            b4_corr,
            b2_corr,
            f"(B > 0.0001) * (A / (B + {eps}))",
            "fe_index",
        )
        clay_path = gdal_calc_2in(
            b6_corr,
            b7_corr,
            f"(B > 0.0001) * (A / (B + {eps}))",
            "clay_index",
        )
        oh_path = gdal_calc_2in(
            b6_corr,
            b5_corr,
            f"(B > 0.0001) * (A / (B + {eps}))",
            "oh_index",
        )

        # 10. enmascarar vegetación
        feedback.pushInfo("Enmascarando vegetación (NDVI < 0.3)...")
        def mask_with_ndvi(idx_path, ndvi_path, name):
            outp = self._unique(out_folder, name)
            processing.run(
                "gdal:rastercalculator",
                {
                    "INPUT_A": idx_path,
                    "BAND_A": 1,
                    "INPUT_B": ndvi_path,
                    "BAND_B": 1,
                    "FORMULA": "A * (B < 0.3)",
                    "NO_DATA": 0.0,
                    "RTYPE": 6,
                    "EXTRA": "",
                    "OPTIONS": "",
                    "OUTPUT": outp,
                },
                context=context,
                feedback=feedback,
            )
            return outp

        fe_masked = mask_with_ndvi(fe_path, ndvi_path, "fe_index_masked")
        clay_masked = mask_with_ndvi(clay_path, ndvi_path, "clay_index_masked")
        oh_masked = mask_with_ndvi(oh_path, ndvi_path, "oh_index_masked")

        feedback.pushInfo("Listo ✅")

        return {
            "NDVI": ndvi_path,
            "FE_INDEX": fe_path,
            "CLAY_INDEX": clay_path,
            "OH_INDEX": oh_path,
            "FE_INDEX_MASKED": fe_masked,
            "CLAY_INDEX_MASKED": clay_masked,
            "OH_INDEX_MASKED": oh_masked,
            "ILLUMINATION": illum_aligned,
        }

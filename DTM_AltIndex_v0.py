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
    QgsRectangle,
)
from qgis import processing
import os
import time


class AlterationIndicesFromLandsatIllum(QgsProcessingAlgorithm):
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
        return QCoreApplication.translate("AlterationIndicesFromLandsatIllum", text)

    def createInstance(self):
        return AlterationIndicesFromLandsatIllum()

    def name(self):
        return "indices_alteracion_landsat_dtm_illum"

    def displayName(self):
        return self.tr(
            "Índices de alteración (Fe, Clay, OH, Propylitic, Silica, Gossan) + DTM (Landsat + DTM + corrección de iluminación)"
        )

    def group(self):
        return self.tr("Geología / Teledetección")

    def groupId(self):
        return "geologia_teledeteccion"

    def shortHelpString(self):
        return self.tr(
            "Genera seis índices de alteración a partir de Landsat 8, corrige las bandas por iluminación derivada de un DTM "
            "y empaqueta los índices en rásteres de 3 bandas. Al final genera un ráster único con DTM + todos los índices."
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
                self.OUT_FOLDER,
                self.tr("Carpeta de salida"),
            )
        )

    # -------- helpers --------
    def _unique(self, out_folder, base):
        ts = time.strftime("%Y%m%d_%H%M%S")
        return os.path.join(out_folder, f"{base}_{ts}.tif")

    def _reproject_extent_rect(self, rect: QgsRectangle, src_crs, dest_crs, context):
        if src_crs == dest_crs:
            return rect
        tr = QgsCoordinateTransform(src_crs, dest_crs, context.transformContext())
        corners = [
            tr.transform(rect.xMinimum(), rect.yMinimum()),
            tr.transform(rect.xMinimum(), rect.yMaximum()),
            tr.transform(rect.xMaximum(), rect.yMinimum()),
            tr.transform(rect.xMaximum(), rect.yMaximum()),
        ]
        xs = [p.x() for p in corners]
        ys = [p.y() for p in corners]
        return QgsRectangle(min(xs), min(ys), max(xs), max(ys))

    # -------- main --------
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

        target_crs = b2_lyr.crs()
        sun_az = self.parameterAsDouble(parameters, self.SUN_AZ, context)
        sun_el = self.parameterAsDouble(parameters, self.SUN_EL, context)

        out_folder = self.parameterAsString(parameters, self.OUT_FOLDER, context)
        if out_folder == "TEMPORARY_OUTPUT":
            out_folder = context.temporaryDirectory()
        if os.path.splitext(out_folder)[1] != "":
            out_folder = os.path.dirname(out_folder)
        os.makedirs(out_folder, exist_ok=True)

        # 2. extent reproyectado al CRS de las bandas
        user_rect, user_crs = None, None
        try:
            user_rect, user_crs = self.parameterAsExtentCrs(parameters, self.EXTENT, context)
        except Exception:
            user_rect = self.parameterAsExtent(parameters, self.EXTENT, context)
            user_crs = context.project().crs() if context.project() else target_crs

        if user_rect is not None and not user_rect.isEmpty():
            feedback.pushInfo("Usando la extensión indicada por el usuario (reproyectada).")
            common_extent = self._reproject_extent_rect(user_rect, user_crs, target_crs, context)
        else:
            feedback.pushInfo("Usando la intersección de todas las bandas.")
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

        # resolución
        resx = b2_lyr.rasterUnitsPerPixelX()

        # 3. reproyectar DTM si hace falta
        if dtm_lyr.crs() != target_crs:
            feedback.pushInfo("DTM en CRS distinto, reproyectando al CRS de las bandas...")
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

        # 4. clip helper
        def clip_to_extent(src_path, name):
            outp = self._unique(out_folder, name)
            processing.run(
                "gdal:cliprasterbyextent",
                {
                    "INPUT": src_path,
                    "PROJWIN": extent_str,
                    "NODATA": 0.0,
                    "OPTIONS": "",
                    "DATA_TYPE": 0,
                    "OUTPUT": outp,
                },
                context=context,
                feedback=feedback,
            )
            return outp

        feedback.pushInfo("Recortando capas al extent (CRS de las bandas)...")
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
                "RTYPE": 6,
                "EXTRA": "",
                "OPTIONS": "",
                "OUTPUT": illum_path,
            },
            context=context,
            feedback=feedback,
        )

        # 7. alinear iluminación
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
                "DATA_TYPE": 6,
                "TARGET_EXTENT": extent_str,
                "TARGET_EXTENT_CRS": target_crs.toWkt(),
                "MULTITHREADING": True,
                "OUTPUT": illum_aligned,
            },
            context=context,
            feedback=feedback,
        )

        eps = 0.0001

        # corrección por iluminación
        def illum_correct(band_path, name):
            outp = self._unique(out_folder, name)
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

        # helper de 2 entradas
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

        # 8. NDVI
        ndvi_path = gdal_calc_2in(
            b5_corr,
            b4_corr,
            f"(A + B > 0.0001) * ((A - B) / (A + B + {eps}))",
            "ndvi",
        )

        # 9. índices de alteración
        index_defs = [
            ("fe_index", f"(B > 0.0001) * (A / (B + {eps}))", b4_corr, b2_corr),
            ("clay_index", f"(B > 0.0001) * (A / (B + {eps}))", b6_corr, b7_corr),
            ("oh_index", f"(B > 0.0001) * (A / (B + {eps}))", b6_corr, b5_corr),
            ("propylitic_index", f"(B > 0.0001) * (A / (B + {eps}))", b5_corr, b4_corr),
            ("silica_index", f"(B > 0.0001) * (A / (B + {eps}))", b6_corr, b2_corr),
            ("gossan_index", f"(B > 0.0001) * (A / (B + {eps}))", b4_corr, b6_corr),
        ]

        index_paths = []
        for name, expr, a_src, b_src in index_defs:
            feedback.pushInfo(f"Calculando {name}...")
            idx_path = gdal_calc_2in(a_src, b_src, expr, name)
            index_paths.append((name, idx_path))

        # 10. empaquetar en ráster de 3 bandas (como antes)
        feedback.pushInfo("Empaquetando índices en ráster de 3 bandas...")
        grouped_outputs = []
        group_size = 3
        for i in range(0, len(index_paths), group_size):
            chunk = index_paths[i : i + group_size]
            merge_out = self._unique(out_folder, f"alteraciones_{i // group_size + 1}")
            processing.run(
                "gdal:merge",
                {
                    "INPUT": [p[1] for p in chunk],
                    "SEPARATE": True,
                    "NODATA_INPUT": 0.0,
                    "NODATA_OUTPUT": 0.0,
                    "PCT": False,
                    "DATA_TYPE": 6,
                    "OUTPUT": merge_out,
                },
                context=context,
                feedback=feedback,
            )
            grouped_outputs.append(merge_out)

        # 11. NUEVO: ráster único con DTM + TODOS los índices
        feedback.pushInfo("Creando ráster único con DTM + todos los índices...")
        full_stack_path = self._unique(out_folder, "dtm_alteraciones_full")
        processing.run(
            "gdal:merge",
            {
                # primero el DTM recortado, luego todos los índices en el mismo orden
                "INPUT": [dtm_clip] + [p[1] for p in index_paths],
                "SEPARATE": True,
                "NODATA_INPUT": 0.0,
                "NODATA_OUTPUT": 0.0,
                "PCT": False,
                "DATA_TYPE": 6,  # Float32
                "OUTPUT": full_stack_path,
            },
            context=context,
            feedback=feedback,
        )

        feedback.pushInfo("Listo ✅")

        out_dict = {
            "NDVI": ndvi_path,
            "ILLUMINATION": illum_aligned,
            "DTM_CLIP": dtm_clip,
            "DTM_ALTERACIONES_FULL": full_stack_path,
        }
        for name, path in index_paths:
            out_dict[name.upper()] = path
        for j, g in enumerate(grouped_outputs, start=1):
            out_dict[f"ALTERACIONES_{j}"] = g

        return out_dict

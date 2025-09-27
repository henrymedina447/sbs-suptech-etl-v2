import asyncio
from typing import List, Set

from mypy_boto3_textract.type_defs import BlockTypeDef


class ExtractAsyncHelper:
    @staticmethod
    def build_index(blocks: list[BlockTypeDef]) -> dict[str, BlockTypeDef]:
        """
        Indexa todos los bloques por Id (O(n))
        :param blocks:
        :return:
        """
        return {b["Id"]: b for b in blocks}

    @staticmethod
    def _children_ids(block: BlockTypeDef, rel_type: str | None = None):
        for rel in block.get("Relationships", []):
            if rel_type is None or rel.get("Type") == rel_type:
                for cid in rel.get("Ids", []):
                    yield cid

    @staticmethod
    def page_closure_ids(page_block: BlockTypeDef, by_id: dict[str, BlockTypeDef]) -> set[str]:
        """
        Recorre relaciones desde PAGE y cierra el conjunto de Ids alcanzables
        :param page_block:
        :param by_id:
        :return:
        """
        seen: Set[str] = {page_block["Id"]}
        stack: List[str] = [*list(ExtractAsyncHelper._children_ids(page_block))]  # hijos directos
        while stack:
            bid = stack.pop()
            if bid in seen or bid not in by_id:
                continue
            seen.add(bid)
            b = by_id[bid]
            # CHILD: LINE→WORD, TABLE→CELL, etc.
            for cid in ExtractAsyncHelper._children_ids(b):
                stack.append(cid)
            # VALUE: KEY_VALUE_SET (KEY → VALUE)
            for vid in ExtractAsyncHelper._children_ids(b, "VALUE"):
                stack.append(vid)
        return seen

    @staticmethod
    def extract_page_text(ids: set[str], by_id: dict[str, BlockTypeDef]) -> dict:
        """
        Extrae contenido por página (ejemplo: texto “LINE→WORD”)
        :param ids:
        :param by_id:
        :return:
        """
        lines = [by_id[i].get("Text", "") for i in ids
                 if i in by_id and by_id[i].get("BlockType") == "LINE"]
        return {"text": "\n".join(lines), "lines_count": len(lines)}

    @staticmethod
    async def extract_pages_async(
            pages: List[BlockTypeDef],
            blocks: List[BlockTypeDef],
            batch_size: int = 4,
            max_concurrency: int = 4,
    ) -> List[dict]:
        """Procesa páginas por lotes, en paralelo limitado (CPU-bound en hilos)."""
        by_id = ExtractAsyncHelper.build_index(blocks)
        sem = asyncio.Semaphore(max_concurrency)

        async def process_one(page_block: BlockTypeDef) -> dict:
            def _work():
                ids = ExtractAsyncHelper.page_closure_ids(page_block, by_id)
                return ExtractAsyncHelper.extract_page_text(ids, by_id)

            # Evita bloquear el event loop si el cierre es pesado
            async with sem:
                return await asyncio.to_thread(_work)

        # chunking por batches
        def chunked(seq: List[BlockTypeDef], n: int):
            for i in range(0, len(seq), n):
                yield seq[i:i + n]

        out: List[dict] = []
        for batch in chunked(pages, batch_size):
            results = await asyncio.gather(*(process_one(p) for p in batch))
            out.extend(results)
        return out


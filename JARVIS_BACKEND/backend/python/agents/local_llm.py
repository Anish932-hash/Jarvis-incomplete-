"""
Local LLM Engine for JARVIS using llama.cpp.

Provides:
- Local inference with Llama 3.2, Qwen2.5, or compatible models
- Function calling support
- Streaming responses
- Multi-modal support (vision models)
- Context management with KV cache
- Efficient batched inference
"""

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

from backend.python.utils.logger import Logger


@dataclass
class LLMMessage:
    """Represents a message in conversation."""
    role: str  # system, user, assistant, tool
    content: str
    name: Optional[str] = None
    tool_calls: Optional[List[Dict[str, Any]]] = None
    tool_call_id: Optional[str] = None


@dataclass
class LLMResponse:
    """Response from LLM inference."""
    content: str
    finish_reason: str  # stop, length, tool_calls
    tool_calls: Optional[List[Dict[str, Any]]] = None
    usage: Dict[str, int] = field(default_factory=dict)
    latency_ms: float = 0.0


@dataclass
class FunctionDefinition:
    """Function definition for function calling."""
    name: str
    description: str
    parameters: Dict[str, Any]
    required: List[str] = field(default_factory=list)


class LocalLLM:
    """
    Local LLM inference engine using llama.cpp.
    
    Supports:
    - Llama 3.2 (8B, 11B Vision)
    - Qwen2.5 (7B, 14B)
    - Mistral models
    - Custom GGUF models
    
    Features:
    - Function calling with automatic JSON extraction
    - Streaming generation
    - Context window management
    - Multi-modal (vision) support
    - GPU acceleration (CUDA, Metal, ROCm)
    """

    def __init__(
        self,
        *,
        model_path: str,
        n_ctx: int = 8192,
        n_gpu_layers: int = -1,
        n_threads: Optional[int] = None,
        temperature: float = 0.7,
        top_p: float = 0.9,
        top_k: int = 40,
        max_tokens: int = 2048,
        repeat_penalty: float = 1.1,
        stop_sequences: Optional[List[str]] = None,
        verbose: bool = False,
    ):
        self.log = Logger("LocalLLM").get_logger()
        
        self.model_path = Path(model_path)
        if not self.model_path.exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
        
        self.n_ctx = n_ctx
        self.n_gpu_layers = n_gpu_layers
        self.n_threads = n_threads or os.cpu_count()
        self.temperature = temperature
        self.top_p = top_p
        self.top_k = top_k
        self.max_tokens = max_tokens
        self.repeat_penalty = repeat_penalty
        self.stop_sequences = stop_sequences or []
        self.verbose = verbose
        
        # Lazy-loaded model
        self._llm = None
        self._model_type = self._detect_model_type()
        self._supports_vision = "vision" in str(model_path).lower() or "llava" in str(model_path).lower()
        
        # Function calling state
        self._functions: Dict[str, FunctionDefinition] = {}
        self._function_call_mode: Optional[str] = None  # auto, none, or function name
        
        self.log.info(f"LocalLLM initialized: {self.model_path.name} (type: {self._model_type})")

    def _detect_model_type(self) -> str:
        """Detect model type from filename."""
        name_lower = str(self.model_path).lower()
        
        if "llama" in name_lower:
            if "3.2" in name_lower or "3.3" in name_lower:
                return "llama-3.2"
            elif "3.1" in name_lower:
                return "llama-3.1"
            return "llama"
        elif "qwen" in name_lower:
            return "qwen2.5"
        elif "mistral" in name_lower:
            return "mistral"
        elif "phi" in name_lower:
            return "phi"
        
        return "unknown"

    def _load_model(self):
        """Lazy load llama.cpp model."""
        if self._llm is not None:
            return self._llm
        
        try:
            from llama_cpp import Llama
            
            self.log.info(f"Loading model: {self.model_path.name}")
            start_time = time.time()
            
            kwargs = {
                "model_path": str(self.model_path),
                "n_ctx": self.n_ctx,
                "n_threads": self.n_threads,
                "n_gpu_layers": self.n_gpu_layers,
                "verbose": self.verbose,
            }
            
            # Vision models need special handling
            if self._supports_vision:
                kwargs["n_batch"] = 512
                kwargs["logits_all"] = True
            
            self._llm = Llama(**kwargs)
            
            load_time = time.time() - start_time
            self.log.info(f"Model loaded in {load_time:.2f}s")
            
            return self._llm
        
        except ImportError:
            self.log.error("llama-cpp-python not installed. Install with: pip install llama-cpp-python")
            raise
        except Exception as exc:
            self.log.error(f"Failed to load model: {exc}")
            raise

    def _format_messages(self, messages: List[LLMMessage]) -> str:
        """Format messages for model-specific prompt template."""
        if self._model_type == "llama-3.2" or self._model_type == "llama-3.1":
            # Llama 3 chat format
            formatted = "<|begin_of_text|>"
            for msg in messages:
                formatted += f"<|start_header_id|>{msg.role}<|end_header_id|>\n\n{msg.content}<|eot_id|>"
            formatted += "<|start_header_id|>assistant<|end_header_id|>\n\n"
            return formatted
        
        elif self._model_type == "qwen2.5":
            # Qwen2.5 chat format
            formatted = ""
            for msg in messages:
                if msg.role == "system":
                    formatted += f"<|im_start|>system\n{msg.content}<|im_end|>\n"
                elif msg.role == "user":
                    formatted += f"<|im_start|>user\n{msg.content}<|im_end|>\n"
                elif msg.role == "assistant":
                    formatted += f"<|im_start|>assistant\n{msg.content}<|im_end|>\n"
            formatted += "<|im_start|>assistant\n"
            return formatted
        
        elif self._model_type == "mistral":
            # Mistral format
            formatted = ""
            for msg in messages:
                if msg.role == "system":
                    formatted += f"[INST] {msg.content} [/INST]\n"
                elif msg.role == "user":
                    formatted += f"[INST] {msg.content} [/INST]\n"
                elif msg.role == "assistant":
                    formatted += f"{msg.content}\n"
            return formatted
        
        else:
            # Generic format
            formatted = ""
            for msg in messages:
                formatted += f"{msg.role.upper()}: {msg.content}\n"
            formatted += "ASSISTANT: "
            return formatted

    def _build_function_calling_prompt(
        self,
        messages: List[LLMMessage],
        functions: List[FunctionDefinition],
    ) -> str:
        """Build prompt for function calling."""
        # Add system message with function definitions
        system_msg = "You are a helpful assistant with access to the following functions:\n\n"
        
        for func in functions:
            system_msg += f"Function: {func.name}\n"
            system_msg += f"Description: {func.description}\n"
            system_msg += f"Parameters: {json.dumps(func.parameters, indent=2)}\n"
            if func.required:
                system_msg += f"Required: {', '.join(func.required)}\n"
            system_msg += "\n"
        
        system_msg += (
            "To call a function, respond with a JSON object in this format:\n"
            '{"function": "function_name", "arguments": {"arg1": "value1", "arg2": "value2"}}\n\n'
            "If you don't need to call a function, respond normally."
        )
        
        # Inject system message
        augmented_messages = [LLMMessage(role="system", content=system_msg)]
        augmented_messages.extend(messages)
        
        return self._format_messages(augmented_messages)

    def generate(
        self,
        messages: List[LLMMessage],
        *,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        stop: Optional[List[str]] = None,
        functions: Optional[List[FunctionDefinition]] = None,
        function_call: Optional[str] = "auto",
    ) -> LLMResponse:
        """
        Generate completion from messages.
        
        Args:
            messages: Conversation history
            temperature: Sampling temperature (overrides default)
            max_tokens: Max tokens to generate (overrides default)
            stop: Additional stop sequences
            functions: Available functions for function calling
            function_call: "auto", "none", or specific function name
            
        Returns:
            LLMResponse with generated content and metadata
        """
        llm = self._load_model()
        start_time = time.time()
        
        # Build prompt
        if functions and function_call != "none":
            prompt = self._build_function_calling_prompt(messages, functions)
        else:
            prompt = self._format_messages(messages)
        
        # Prepare generation params
        gen_temp = temperature if temperature is not None else self.temperature
        gen_max_tokens = max_tokens if max_tokens is not None else self.max_tokens
        gen_stop = list(self.stop_sequences)
        if stop:
            gen_stop.extend(stop)
        
        # Generate
        try:
            output = llm(
                prompt,
                max_tokens=gen_max_tokens,
                temperature=gen_temp,
                top_p=self.top_p,
                top_k=self.top_k,
                repeat_penalty=self.repeat_penalty,
                stop=gen_stop if gen_stop else None,
                echo=False,
            )
            
            content = output["choices"][0]["text"].strip()
            finish_reason = output["choices"][0]["finish_reason"]
            
            # Extract function calls if present
            tool_calls = None
            if functions and function_call != "none":
                extracted = self._extract_function_call(content)
                if extracted:
                    tool_calls = [extracted]
                    finish_reason = "tool_calls"
            
            # Calculate usage
            usage = {
                "prompt_tokens": output["usage"]["prompt_tokens"],
                "completion_tokens": output["usage"]["completion_tokens"],
                "total_tokens": output["usage"]["total_tokens"],
            }
            
            latency_ms = (time.time() - start_time) * 1000
            
            return LLMResponse(
                content=content,
                finish_reason=finish_reason,
                tool_calls=tool_calls,
                usage=usage,
                latency_ms=latency_ms,
            )
        
        except Exception as exc:
            self.log.error(f"Generation failed: {exc}")
            raise

    def generate_stream(
        self,
        messages: List[LLMMessage],
        *,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        stop: Optional[List[str]] = None,
    ) -> Iterator[str]:
        """
        Generate completion with streaming.
        
        Args:
            messages: Conversation history
            temperature: Sampling temperature
            max_tokens: Max tokens to generate
            stop: Additional stop sequences
            
        Yields:
            Token strings as they are generated
        """
        llm = self._load_model()
        
        prompt = self._format_messages(messages)
        
        gen_temp = temperature if temperature is not None else self.temperature
        gen_max_tokens = max_tokens if max_tokens is not None else self.max_tokens
        gen_stop = list(self.stop_sequences)
        if stop:
            gen_stop.extend(stop)
        
        try:
            stream = llm(
                prompt,
                max_tokens=gen_max_tokens,
                temperature=gen_temp,
                top_p=self.top_p,
                top_k=self.top_k,
                repeat_penalty=self.repeat_penalty,
                stop=gen_stop if gen_stop else None,
                stream=True,
                echo=False,
            )
            
            for output in stream:
                chunk = output["choices"][0]["text"]
                if chunk:
                    yield chunk
        
        except Exception as exc:
            self.log.error(f"Streaming generation failed: {exc}")
            raise

    def _extract_function_call(self, content: str) -> Optional[Dict[str, Any]]:
        """Extract function call from model output."""
        # Look for JSON object
        try:
            # Try to find JSON in the content
            start_idx = content.find("{")
            end_idx = content.rfind("}") + 1
            
            if start_idx != -1 and end_idx > start_idx:
                json_str = content[start_idx:end_idx]
                parsed = json.loads(json_str)
                
                if "function" in parsed and "arguments" in parsed:
                    return {
                        "type": "function",
                        "function": {
                            "name": parsed["function"],
                            "arguments": json.dumps(parsed["arguments"]),
                        },
                    }
        
        except json.JSONDecodeError:
            pass
        
        return None

    def register_function(self, func: FunctionDefinition):
        """Register a function for function calling."""
        self._functions[func.name] = func
        self.log.debug(f"Registered function: {func.name}")

    def unregister_function(self, name: str):
        """Unregister a function."""
        if name in self._functions:
            del self._functions[name]
            self.log.debug(f"Unregistered function: {name}")

    def get_registered_functions(self) -> List[FunctionDefinition]:
        """Get all registered functions."""
        return list(self._functions.values())

    def embed(self, text: str) -> List[float]:
        """Generate embeddings for text (if model supports it)."""
        llm = self._load_model()
        
        try:
            embedding = llm.embed(text)
            return embedding
        except Exception as exc:
            self.log.error(f"Embedding generation failed: {exc}")
            raise

    def count_tokens(self, text: str) -> int:
        """Count tokens in text."""
        llm = self._load_model()
        
        try:
            tokens = llm.tokenize(text.encode("utf-8"))
            return len(tokens)
        except Exception as exc:
            self.log.error(f"Token counting failed: {exc}")
            return len(text.split())  # Fallback approximation

    def reset_cache(self):
        """Reset KV cache."""
        if self._llm is not None:
            try:
                self._llm.reset()
                self.log.debug("KV cache reset")
            except Exception as exc:
                self.log.warning(f"Failed to reset cache: {exc}")

    @property
    def supports_vision(self) -> bool:
        """Check if model supports vision inputs."""
        return self._supports_vision

    @property
    def model_type(self) -> str:
        """Get detected model type."""
        return self._model_type

    @property
    def context_length(self) -> int:
        """Get context window size."""
        return self.n_ctx

    def close(self) -> None:
        """Release the loaded llama.cpp runtime if supported."""
        if self._llm is None:
            return
        try:
            close_fn = getattr(self._llm, "close", None)
            if callable(close_fn):
                close_fn()
        except Exception as exc:
            self.log.warning(f"Failed to close local model runtime: {exc}")
        finally:
            self._llm = None

    def health_check(self) -> Dict[str, Any]:
        """Check model health."""
        try:
            llm = self._load_model()
            
            # Try a simple generation
            test_prompt = "Hello"
            result = llm(test_prompt, max_tokens=5)
            
            return {
                "status": "healthy",
                "model": self.model_path.name,
                "model_type": self._model_type,
                "context_length": self.n_ctx,
                "gpu_layers": self.n_gpu_layers,
                "supports_vision": self._supports_vision,
                "test_generation": result["choices"][0]["text"],
            }
        
        except Exception as exc:
            return {
                "status": "unhealthy",
                "error": str(exc),
            }


class LocalLLMPool:
    """
    Pool of local LLM instances for parallel inference.
    
    Useful for handling multiple concurrent requests efficiently.
    """

    def __init__(
        self,
        model_path: str,
        pool_size: int = 2,
        **llm_kwargs,
    ):
        self.log = Logger("LocalLLMPool").get_logger()
        self.model_path = model_path
        self.pool_size = pool_size
        self.llm_kwargs = llm_kwargs
        
        self._pool: List[LocalLLM] = []
        self._available: List[bool] = []
        
        # Lazy initialization
        self._initialized = False
        
        self.log.info(f"LocalLLMPool created with size {pool_size}")

    def _initialize_pool(self):
        """Initialize all LLM instances in pool."""
        if self._initialized:
            return
        
        self.log.info("Initializing LLM pool...")
        for i in range(self.pool_size):
            llm = LocalLLM(model_path=self.model_path, **self.llm_kwargs)
            self._pool.append(llm)
            self._available.append(True)
        
        self._initialized = True
        self.log.info(f"Pool initialized with {self.pool_size} instances")

    def acquire(self) -> Optional[LocalLLM]:
        """Acquire an available LLM from pool."""
        self._initialize_pool()
        
        for i, available in enumerate(self._available):
            if available:
                self._available[i] = False
                return self._pool[i]
        
        return None  # All instances busy

    def release(self, llm: LocalLLM):
        """Release LLM back to pool."""
        for i, instance in enumerate(self._pool):
            if instance is llm:
                self._available[i] = True
                return
        
        self.log.warning("Attempted to release LLM not from this pool")

    def get_available_count(self) -> int:
        """Get number of available instances."""
        return sum(self._available) if self._initialized else self.pool_size

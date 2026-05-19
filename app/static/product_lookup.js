(() => {
  const lookupInputs = document.querySelectorAll("[data-product-lookup-url]");
  if (!lookupInputs.length) {
    return;
  }

  const debounce = (fn, waitMs) => {
    let timeoutId = null;
    return (...args) => {
      if (timeoutId) {
        window.clearTimeout(timeoutId);
      }
      timeoutId = window.setTimeout(() => fn(...args), waitMs);
    };
  };

  const buildLabel = (product) => {
    const primaryText = (product.name || product.description || "").trim();
    return [product.sku || "", primaryText].filter(Boolean).join(" - ");
  };

  const buildSecondaryText = (product) => {
    const description = (product.description || "").trim();
    const name = (product.name || "").trim();
    if (description && description !== name) {
      return description;
    }
    return "";
  };

  const hideResults = (resultsEl) => {
    if (!resultsEl) {
      return;
    }
    resultsEl.hidden = true;
    resultsEl.replaceChildren();
  };

  const renderMessage = (resultsEl, message) => {
    resultsEl.replaceChildren();
    const text = document.createElement("p");
    text.className = "muted-inline product-lookup-empty";
    text.textContent = message;
    resultsEl.appendChild(text);
    resultsEl.hidden = false;
  };

  const getNamedControlValue = (form, name) => {
    if (!form || !name) {
      return "";
    }
    const control = form.elements.namedItem(name);
    if (!control) {
      return "";
    }
    if (typeof control.value === "string") {
      return control.value;
    }
    if (typeof control.length === "number" && control.length > 0 && typeof control[0]?.value === "string") {
      return control[0].value;
    }
    return "";
  };

  lookupInputs.forEach((inputEl) => {
    const resultsEl = document.getElementById(inputEl.dataset.productLookupResultsId || "");
    const previewEl = document.getElementById(inputEl.dataset.productLookupPreviewId || "");
    const mode = inputEl.dataset.productLookupMode || "sku";
    const targetSelector = inputEl.dataset.productLookupTarget || "";
    const targetEl = targetSelector ? document.querySelector(targetSelector) : inputEl;
    const extraParamNames = (inputEl.dataset.productLookupExtraParams || "")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);

    let requestId = 0;
    let selectedSku = "";

    const setPreview = (text) => {
      if (previewEl) {
        previewEl.textContent = text;
      }
    };

    const runLookup = async () => {
      if (!resultsEl) {
        return;
      }

      const q = inputEl.value.trim();
      if (q.length < 2) {
        hideResults(resultsEl);
        if (q !== selectedSku) {
          setPreview("");
        }
        return;
      }

      const currentRequest = ++requestId;
      const url = new URL(inputEl.dataset.productLookupUrl, window.location.origin);
      url.searchParams.set("q", q);
      extraParamNames.forEach((name) => {
        const value = getNamedControlValue(inputEl.form, name).trim();
        if (value) {
          url.searchParams.set(name, value);
        } else {
          url.searchParams.delete(name);
        }
      });

      let response;
      try {
        response = await fetch(url.toString(), {
          headers: { "X-Requested-With": "fetch" },
        });
      } catch (_error) {
        renderMessage(resultsEl, "Lookup unavailable right now.");
        return;
      }

      if (!response.ok || currentRequest !== requestId) {
        return;
      }

      const products = await response.json();
      if (currentRequest !== requestId) {
        return;
      }

      resultsEl.replaceChildren();
      if (!Array.isArray(products) || !products.length) {
        renderMessage(resultsEl, "No matching products found.");
        return;
      }

      products.forEach((product) => {
        const label = buildLabel(product);
        if (!label) {
          return;
        }

        const button = document.createElement("button");
        button.type = "button";
        button.className = "lookup-result-button";

        const strong = document.createElement("strong");
        strong.textContent = label;
        button.appendChild(strong);

        const secondaryText = buildSecondaryText(product);
        if (secondaryText) {
          const span = document.createElement("span");
          span.textContent = secondaryText;
          button.appendChild(span);
        }

        button.addEventListener("click", () => {
          const nextValue = mode === "sku" ? (product.sku || "") : label;
          if (targetEl) {
            targetEl.value = nextValue;
          }
          inputEl.value = nextValue;
          selectedSku = product.sku || "";
          setPreview(`Selected: ${label}`);
          hideResults(resultsEl);
        });

        resultsEl.appendChild(button);
      });

      if (!resultsEl.childElementCount) {
        renderMessage(resultsEl, "No matching products found.");
        return;
      }

      resultsEl.hidden = false;
    };

    const debouncedLookup = debounce(runLookup, 180);

    inputEl.addEventListener("input", () => {
      if (inputEl.value.trim() !== selectedSku) {
        setPreview("");
      }
      debouncedLookup();
    });

    inputEl.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        hideResults(resultsEl);
      }
    });

    inputEl.addEventListener("blur", () => {
      window.setTimeout(() => hideResults(resultsEl), 120);
    });
  });
})();

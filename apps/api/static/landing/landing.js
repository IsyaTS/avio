(() => {
  document.body.classList.add("has-js");
  const prefersReduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const header = document.querySelector(".site-header");
  const navToggle = document.querySelector(".nav-toggle");
  const navOverlay = document.querySelector(".nav-overlay");
  const navPanel = document.querySelector(".nav-panel");
  const registerForm = document.querySelector(".js-register-form");

  const setHeaderState = () => {
    if (!header) return;
    header.classList.toggle("is-scrolled", window.scrollY > 8);
  };

  setHeaderState();
  window.addEventListener("scroll", setHeaderState, { passive: true });
  window.addEventListener("resize", () => {
    if (window.innerWidth > 960) {
      closeMenu();
    }
  });

  const closeMenu = () => {
    document.body.classList.remove("nav-open");
    if (navToggle) navToggle.setAttribute("aria-expanded", "false");
  };

  const openMenu = () => {
    document.body.classList.add("nav-open");
    if (navToggle) navToggle.setAttribute("aria-expanded", "true");
  };

  if (navToggle) {
    navToggle.addEventListener("click", () => {
      if (document.body.classList.contains("nav-open")) {
        closeMenu();
      } else {
        openMenu();
      }
    });
  }

  if (navOverlay) {
    navOverlay.addEventListener("click", closeMenu);
  }

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeMenu();
    }
  });

  const navLinks = Array.from(document.querySelectorAll(".site-nav a[href*='#']"));
  const sections = [];
  const linkMap = new Map();

  navLinks.forEach((link) => {
    const href = link.getAttribute("href") || "";
    const hash = href.split("#")[1];
    if (!hash) return;
    const section = document.getElementById(hash);
    if (section) {
      sections.push(section);
      linkMap.set(section, link);
    }

    link.addEventListener("click", (event) => {
      const url = new URL(link.href, window.location.origin);
      if (url.pathname === window.location.pathname && hash) {
        const target = document.getElementById(hash);
        if (target) {
          event.preventDefault();
          target.scrollIntoView({
            behavior: prefersReduced ? "auto" : "smooth",
            block: "start",
          });
          history.replaceState(null, "", `#${hash}`);
          closeMenu();
        }
      } else {
        closeMenu();
      }
    });
  });

  if (sections.length) {
    const spyObserver = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          const link = linkMap.get(entry.target);
          if (!link) return;
          if (entry.isIntersecting) {
            linkMap.forEach((value) => value.classList.remove("active"));
            link.classList.add("active");
          }
        });
      },
      { rootMargin: "-30% 0px -60% 0px", threshold: 0.1 }
    );

    sections.forEach((section) => spyObserver.observe(section));
  }

  const revealItems = document.querySelectorAll("[data-reveal]");
  if (prefersReduced) {
    revealItems.forEach((item) => item.classList.add("is-visible"));
  } else {
    const revealObserver = new IntersectionObserver(
      (entries, observer) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.classList.add("is-visible");
            observer.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.2 }
    );

    revealItems.forEach((item) => revealObserver.observe(item));
  }

  if (!prefersReduced) {
    const orbs = Array.from(document.querySelectorAll("[data-orb]"));
    let ticking = false;

    const updateOrbs = () => {
      const scrollY = window.scrollY || 0;
      orbs.forEach((orb) => {
        const speed = parseFloat(orb.getAttribute("data-speed") || "0.2");
        orb.style.transform = `translate3d(0, ${scrollY * speed * 0.08}px, 0)`;
      });
      ticking = false;
    };

    const onScroll = () => {
      if (!ticking) {
        window.requestAnimationFrame(updateOrbs);
        ticking = true;
      }
    };

    window.addEventListener("scroll", onScroll, { passive: true });
    updateOrbs();
  }

  if (navPanel) {
    navPanel.addEventListener("click", (event) => {
      const target = event.target;
      if (target instanceof HTMLElement && target.tagName === "A") {
        closeMenu();
      }
    });
  }

  if (registerForm) {
    const requiredNames = ["email", "phone", "password"];
    const extraInputs = Array.from(
      registerForm.querySelectorAll("[data-extra-fields] input, [data-extra-fields] select")
    );
    const updateExtra = () => {
      const ready = requiredNames.every((name) => {
        const input = registerForm.querySelector(`[name=\"${name}\"]`);
        return input && String(input.value || \"\").trim().length > 0;
      });
      registerForm.classList.toggle(\"is-expanded\", ready);
      extraInputs.forEach((field) => {
        field.disabled = !ready;
      });
    };

    registerForm.addEventListener(\"input\", updateExtra);
    updateExtra();
  }
})();
